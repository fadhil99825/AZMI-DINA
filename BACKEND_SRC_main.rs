use anyhow::{anyhow, Result};
use dotenvy::dotenv;
use log::{debug, error, info, warn};
use reqwest::Client;
use rumqttc::{
    AsyncClient as MqttClient,
    Event,
    Incoming,
    MqttOptions,
    QoS,
    TlsConfiguration,
    Transport,
};
use rustls_pki_types::CertificateDer;
use serde_json::json;
use std::{
    env,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{
        Duration,
        SystemTime,
        UNIX_EPOCH
    },
};
use tokio::sync::mpsc;

mod serial;
use crate::serial::{SensorData, SerialMonitor};

#[allow(dead_code)]
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Write sensor data including motor and pump status to InfluxDB
async fn write_to_influxdb(
    client: &Client,
    bucket: &str,
    influx_url: &str,
    org: &str,
    token: &str,
    data: &SensorData,
) -> Result<()> {
    let url = format!(
        "{}/api/v2/write?org={}&bucket={}&precision=ns",
        influx_url,
        org,
        bucket
    );
    
    // Convert Option<bool> to integer: Some(true)=1, Some(false)=0, None=-1
    let motor_val = data.motor_status.map(|b| if b { 1 } else { 0 }).unwrap_or(-1);
    let pump_val = data.pump_status.map(|b| if b { 1 } else { 0 }).unwrap_or(-1);
    
    let body = format!(
        "sht20_sensor,host=esp32 temperature={},humidity={},motor_status={},pump_status={} {}",
        data.temperature,
        data.humidity,
        motor_val,
        pump_val,
        data.timestamp
    );

    debug!("Writing to InfluxDB: {}", body);

    let resp = client
        .post(&url)
        .header("Authorization", format!("Token {}", token.trim()))
        .header("Content-Type", "text/plain")
        .body(body)
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let error_body = resp.text().await.unwrap_or_default();
        return Err(anyhow!(
            "InfluxDB write FAILED: {} | {}",
            status,
            error_body.trim()
        ));
    }

    info!("✅ Sensor data + relay status written to InfluxDB");
    Ok(())
}

/// Execute Flux query against InfluxDB
async fn post_influx(
    client: &Client,
    flux: String,
    influx_url: &str,
    org: &str,
    token: &str,
) -> Result<String> {
    let url = format!("{}/api/v2/query?org={}", influx_url, org);
    debug!("Executing Flux query: {}", flux);

    let resp = client
        .post(&url)
        .header("Authorization", format!("Token {}", token.trim()))
        .header("Accept", "application/csv")
        .header("Content-Type", "application/vnd.flux")
        .body(flux)
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(anyhow!(
            "Influx query FAILED: {} | {}",
            status,
            body.trim()
        ));
    }

    if !body.trim().is_empty() {
        debug!("InfluxDB CSV Response: {} bytes", body.len());
    } else {
        warn!("InfluxDB returned empty response");
    }

    Ok(body)
}

#[derive(Default, Debug, Clone, Copy)]
struct LastRow {
    temp: Option<f64>,
    hum: Option<f64>,
    motor: Option<i64>,
    pump: Option<i64>,
}

/// Query last data from InfluxDB including relay status
async fn get_last_data(
    client: &Client,
    bucket: &str,
    measurement: &str,
    range: &str,
    window: &str,
    influx_url: &str,
    org: &str,
    token: &str,
) -> Result<LastRow> {
    let query = format!(
        r#"from(bucket: "{}")
  |> range(start: {})
  |> filter(fn: (r) => r["_measurement"] == "{}")
  |> filter(fn: (r) => 
      r["_field"] == "temperature" or 
      r["_field"] == "temperature_celsius" or 
      r["_field"] == "humidity" or
      r["_field"] == "motor_status" or
      r["_field"] == "pump_status"
  )
  |> aggregateWindow(every: {}, fn: mean, createEmpty: false)
  |> group(columns: ["_field"])
  |> last()"#,
        bucket, range, measurement, window
    );

    let csv = post_influx(client, query, influx_url, org, token).await?;
    Ok(parse_influx_csv(&csv))
}

/// Parse CSV response from InfluxDB
fn parse_influx_csv(csv: &str) -> LastRow {
    let mut idx_field: Option<usize> = None;
    let mut idx_value: Option<usize> = None;
    let mut header_seen = false;
    let mut out = LastRow::default();

    debug!("Parsing CSV with {} lines", csv.lines().count());

    for (line_num, line) in csv.lines().enumerate() {
        if line.starts_with('#') {
            debug!("Skipping comment line {}: {}", line_num, line);
            continue;
        }

        let cols: Vec<&str> = line.split(',').collect();

        if !header_seen && (cols.contains(&"_field") || cols.contains(&"_value")) {
            debug!("Found header line {}: {:?}", line_num, cols);
            for (i, c) in cols.iter().enumerate() {
                if *c == "_field" {
                    idx_field = Some(i);
                }
                if *c == "_value" {
                    idx_value = Some(i);
                }
            }
            header_seen = true;
            continue;
        }

        if header_seen && !line.trim().is_empty() {
            if let (Some(i_f), Some(i_v)) = (idx_field, idx_value) {
                if i_f < cols.len() && i_v < cols.len() {
                    let fname = cols[i_f].trim();
                    let val_str = cols[i_v].trim();

                    debug!("Processing field '{}' with value '{}'", fname, val_str);

                    match fname {
                        "temperature" | "temperature_celsius" => {
                            if let Ok(val) = val_str.parse::<f64>() {
                                out.temp = Some(val);
                                debug!("Found temperature: {:.2}C", val);
                            }
                        }
                        "humidity" => {
                            if let Ok(val) = val_str.parse::<f64>() {
                                out.hum = Some(val);
                                debug!("Found humidity: {:.2}%", val);
                            }
                        }
                        "motor_status" => {
                            if let Ok(val) = val_str.parse::<i64>() {
                                out.motor = Some(val);
                                debug!("Found motor_status: {}", val);
                            }
                        }
                        "pump_status" => {
                            if let Ok(val) = val_str.parse::<i64>() {
                                out.pump = Some(val);
                                debug!("Found pump_status: {}", val);
                            }
                        }
                        _ => {
                            debug!("Ignoring field: {}", fname);
                        }
                    }
                }
            }
        }
    }

    debug!("Parsed data: {:?}", out);
    out
}

/// Get all sensor data including relay status and prepare JSON payload
async fn get_all_sensor_data(
    client: &Client,
    sensor_bucket: &str,
    dwsim_bucket: &str,
    range: &str,
    window: &str,
    influx_url: &str,
    org: &str,
    token: &str,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut payload = serde_json::Map::new();

    // Query SHT20 Sensor (temperature, humidity, motor_status, pump_status)
    info!("Querying SHT20 sensor data from bucket: {}", sensor_bucket);
    match get_last_data(
        client,
        sensor_bucket,
        "sht20_sensor",
        range,
        window,
        influx_url,
        org,
        token,
    ).await {
        Ok(data) => {
            if let Some(t) = data.temp {
                payload.insert("sht20_temperature".into(), json!(t));
                info!("SHT20 Temperature: {:.2}C", t);
            }
            if let Some(h) = data.hum {
                payload.insert("sht20_humidity".into(), json!(h));
                info!("SHT20 Humidity: {:.2}%", h);
            }
            
            // FIXED: Send integer values (0/1/-1) instead of boolean
            // This allows ThingsBoard to distinguish between:
            // -1 = No data available
            //  0 = OFF
            //  1 = ON
            if let Some(m) = data.motor {
                payload.insert("motor_status".into(), json!(m));
                info!("Motor Status: {} ({})", m, match m {
                    1 => "ON",
                    0 => "OFF",
                    _ => "NO DATA"
                });
            }
            if let Some(p) = data.pump {
                payload.insert("pump_status".into(), json!(p));
                info!("Pump Status: {} ({})", p, match p {
                    1 => "ON",
                    0 => "OFF",
                    _ => "NO DATA"
                });
            }
        }
        Err(e) => {
            warn!("Failed to query SHT20 data: {}", e);
        }
    }

    // Query DWSIM Temperature
    info!("Querying DWSIM temperature data from bucket: {}", dwsim_bucket);
    match get_last_data(
        client,
        dwsim_bucket,
        "dwsim_temperature",
        range,
        window,
        influx_url,
        org,
        token,
    ).await {
        Ok(data) => {
            if let Some(t) = data.temp {
                payload.insert("dwsim_temperature".into(), json!(t));
                info!("DWSIM Temperature: {:.2}C", t);
            } else {
                warn!("DWSIM temperature field is empty");
            }
        }
        Err(e) => {
            warn!("Failed to query DWSIM temperature: {}", e);
        }
    }

    Ok(payload)
}

#[allow(dead_code)]
async fn check_mqtt_connection(client: &MqttClient) -> bool {
    match client
        .publish(
            "v1/devices/me/attributes",
            QoS::AtMostOnce,
            false,
            r###"{"connection_test": true}"###,
        )
        .await
    {
        Ok(_) => {
            debug!("MQTT connection health check passed");
            true
        }
        Err(e) => {
            warn!("MQTT connection health check failed: {:?}", e);
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    env_logger::init();

    info!("========================================");
    info!("Starting Backend v2.2 (RELAY STATUS FIXED)");
    info!("Serial-to-InfluxDB & Influx-to-ThingsBoard");
    info!("Motor/Pump Status: Integer (0=OFF, 1=ON, -1=NO DATA)");
    info!("========================================");

    // --- Load Configuration ---
    let influx_url = Arc::new(env::var("INFLUXDB_URL").expect("INFLUXDB_URL must be set"));
    let influx_org = Arc::new(env::var("INFLUXDB_ORG").expect("INFLUXDB_ORG must be set"));
    let influx_token = Arc::new(env::var("INFLUXDB_TOKEN").expect("INFLUXDB_TOKEN must be set"));
    let sensor_bucket = Arc::new(env::var("SENSOR_BUCKET").expect("SENSOR_BUCKET must be set"));
    let dwsim_bucket = Arc::new(env::var("DWSIM_BUCKET").expect("DWSIM_BUCKET must be set"));
    let serial_port = env::var("SERIAL_PORT").expect("SERIAL_PORT must be set");
    let baud_rate = env::var("BAUD_RATE")
        .unwrap_or_else(|_| "9600".to_string())
        .parse::<u32>()?;

    let tb_token =
        env::var("THINGSBOARD_DEVICE_TOKEN").expect("THINGSBOARD_DEVICE_TOKEN must be set");
    let tb_mqtt_url = env::var("THINGSBOARD_MQTT_URL").expect("THINGSBOARD_MQTT_URL must be set");
    let query_interval = env::var("QUERY_INTERVAL")
        .unwrap_or_else(|_| "10".to_string())
        .parse::<u64>()
        .unwrap_or(10);

    info!("Configuration loaded:");
    info!("  - Serial Port: {} @ {} baud", serial_port, baud_rate);
    info!("  - InfluxDB URL: {}", influx_url.as_str());
    info!("  - InfluxDB Org: {}", influx_org.as_str());
    info!("  - Sensor Bucket: {}", sensor_bucket.as_str());
    info!("  - DWSIM Bucket: {}", dwsim_bucket.as_str());
    info!("  - ThingsBoard URL: {}", tb_mqtt_url);
    info!("  - Query Interval: {} seconds", query_interval);

    // --- Initialize HTTP Client ---
    let http_client = Arc::new(
        Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(30))
            .build()?,
    );

    // --- Channel for Serial Data ---
    let (tx, mut rx) = mpsc::channel::<SensorData>(100);
    let data_sent_counter = Arc::new(AtomicU64::new(0));
    let data_received_counter = Arc::new(AtomicU64::new(0));

    // --- Task 1: Serial Port Monitoring ---
    tokio::spawn({
        let tx = tx.clone();
        let counter = data_sent_counter.clone();
        async move {
            info!("🔌 SERIAL_TASK: Starting...");
            let serial_monitor = SerialMonitor::new(serial_port.clone(), baud_rate);

            let on_data = move |data: SensorData| {
                let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
                info!(
                    "📡 SERIAL_TASK [#{}]: T={:.1}°C, H={:.1}%, Motor={:?}, Pump={:?}",
                    count, data.temperature, data.humidity, 
                    data.motor_status, data.pump_status
                );
                
                match tx.try_send(data) {
                    Ok(_) => {
                        info!("✅ SERIAL_TASK [#{}]: Sent to channel", count);
                        Ok(())
                    }
                    Err(e) => {
                        error!("❌ SERIAL_TASK [#{}]: Channel error: {}", count, e);
                        Err(anyhow!("Channel send error: {}", e))
                    }
                }
            };

            if let Err(e) = serial_monitor.start_monitoring(on_data).await {
                error!("❌ SERIAL_TASK: Monitoring failed: {}", e);
            }
        }
    });

    // --- Task 2: InfluxDB Writer ---
    let influx_url_writer = influx_url.clone();
    let influx_org_writer = influx_org.clone();
    let influx_token_writer = influx_token.clone();
    let sensor_bucket_writer = sensor_bucket.clone();
    let http_client_writer = http_client.clone();

    tokio::spawn({
        let counter = data_received_counter.clone();
        async move {
            info!("💾 INFLUX_WRITER_TASK: Starting...");
            while let Some(data) = rx.recv().await {
                let count = counter.fetch_add(1, Ordering::Relaxed) + 1;
                info!(
                    "📥 INFLUX_WRITER_TASK [#{}]: T={:.1}°C, H={:.1}%, Motor={:?}, Pump={:?}",
                    count, data.temperature, data.humidity,
                    data.motor_status, data.pump_status
                );
                
                match write_to_influxdb(
                    &http_client_writer,
                    &sensor_bucket_writer,
                    &influx_url_writer,
                    &influx_org_writer,
                    &influx_token_writer,
                    &data,
                )
                .await {
                    Ok(_) => {
                        info!("✅ INFLUX_WRITER_TASK [#{}]: Write successful!", count);
                    }
                    Err(e) => {
                        error!("❌ INFLUX_WRITER_TASK [#{}]: Failed to write: {}", count, e);
                    }
                }
            }
            warn!("⚠️  INFLUX_WRITER_TASK: Channel closed. Shutting down.");
        }
    });

    // Statistics reporter
    tokio::spawn({
        let sent = data_sent_counter.clone();
        let received = data_received_counter.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let s = sent.load(Ordering::Relaxed);
                let r = received.load(Ordering::Relaxed);
                info!("📊 STATS: Serial→Channel: {} | Channel→InfluxDB: {}", s, r);
                if s > r {
                    warn!("⚠️  STATS: {} data points lost in channel!", s - r);
                }
            }
        }
    });

    // Test InfluxDB connection
    info!("🔍 Testing InfluxDB connection...");
    match http_client
        .get(&format!("{}/ping", influx_url.as_str()))
        .send()
        .await
    {
        Ok(resp) => {
            info!("✅ InfluxDB connection test: Status {}", resp.status());
        }
        Err(e) => {
            error!("❌ InfluxDB connection test failed: {}", e);
        }
    }

    // --- Task 3: ThingsBoard Bridge ---
    const RANGE: &str = "-1h";
    const WINDOW: &str = "1m";

    info!("🌉 BRIDGE_TASK: Configuration:");
    info!("  - ThingsBoard MQTT URL: {}", tb_mqtt_url);
    info!("  - Query Interval: {} seconds", query_interval);

    let is_tls = tb_mqtt_url.starts_with("mqtts://");
    let tb_mqtt_uri = tb_mqtt_url
        .strip_prefix("mqtts://")
        .or_else(|| tb_mqtt_url.strip_prefix("mqtt://"))
        .unwrap_or(&tb_mqtt_url);

    let mut tb_parts = tb_mqtt_uri.split(':');
    let tb_host = tb_parts.next().unwrap_or("demo.thingsboard.io");
    let tb_port = tb_parts
        .next()
        .unwrap_or(if is_tls { "8883" } else { "1883" })
        .parse::<u16>()?;

    info!("🌉 BRIDGE_TASK: MQTT Connection Details:");
    info!("  - Host: {}", tb_host);
    info!("  - Port: {}", tb_port);
    info!("  - TLS: {}", if is_tls { "Enabled" } else { "Disabled" });

    let mut mqtt_options = MqttOptions::new("rust-bridge", tb_host, tb_port);
    mqtt_options.set_credentials(tb_token.clone(), "");
    mqtt_options.set_keep_alive(Duration::from_secs(30));

    if is_tls {
        info!("🔒 BRIDGE_TASK: Setting up secure TLS configuration...");
        let mut root_cert_store = rustls::RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs()? {
            root_cert_store.add(CertificateDer::from(cert))?;
        }
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(root_cert_store))
            .with_no_client_auth();
        let transport = Transport::tls_with_config(TlsConfiguration::Rustls(Arc::new(client_config)));
        mqtt_options.set_transport(transport);
        info!("✅ BRIDGE_TASK: Secure TLS configuration applied.");
    }

    let (mqtt_client, eventloop) = MqttClient::new(mqtt_options, 10);
    let mqtt_connected = Arc::new(AtomicBool::new(false));
    let mqtt_connected_clone = mqtt_connected.clone();

    tokio::spawn(async move {
        let mut eventloop = eventloop;
        let mut connection_attempts = 0;
        info!("📡 MQTT_EVENT_LOOP: Starting...");
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                    info!("✅ MQTT_EVENT_LOOP: MQTT connected to ThingsBoard: {:?}", connack);
                    connection_attempts = 0;
                    mqtt_connected_clone.store(true, Ordering::Relaxed);
                }
                Ok(Event::Incoming(Incoming::Disconnect)) => {
                    warn!("⚠️  MQTT_EVENT_LOOP: MQTT disconnected.");
                    mqtt_connected_clone.store(false, Ordering::Relaxed);
                }
                Err(e) => {
                    error!("❌ MQTT_EVENT_LOOP: Error: {:#?}", e);
                    mqtt_connected_clone.store(false, Ordering::Relaxed);
                    let backoff = std::cmp::min(300, 5 * 2_u64.pow(connection_attempts.min(5)));
                    warn!("🔄 MQTT_EVENT_LOOP: Retrying in {}s...", backoff);
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    connection_attempts += 1;
                }
                _ => {}
            }
        }
    });

    info!("⏳ BRIDGE_TASK: Waiting for initial MQTT connection...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut loop_counter = 0;
    loop {
        loop_counter += 1;
        info!("");
        info!("========================================");
        info!("🌉 BRIDGE_TASK: [Loop {}] Querying InfluxDB...", loop_counter);

        let is_connected = mqtt_connected.load(Ordering::Relaxed);
        info!("📡 BRIDGE_TASK: MQTT Connection Status: {}", if is_connected { "✅ Connected" } else { "❌ Disconnected" });

        match get_all_sensor_data(
            &http_client,
            &sensor_bucket,
            &dwsim_bucket,
            RANGE,
            WINDOW,
            &influx_url,
            &influx_org,
            &influx_token,
        )
        .await
        {
            Ok(payload) => {
                let data_found = !payload.is_empty();

                if !data_found {
                    warn!("⚠️  BRIDGE_TASK: No sensor data retrieved from InfluxDB.");
                } else {
                    info!("📊 BRIDGE_TASK: Retrieved {} field(s) from InfluxDB", payload.len());
                    
                    // Log relay status if present
                    if let Some(motor) = payload.get("motor_status") {
                        if let Some(val) = motor.as_i64() {
                            info!("  🔌 Motor: {} ({})", val, match val {
                                1 => "ON",
                                0 => "OFF",
                                _ => "NO DATA"
                            });
                        }
                    }
                    if let Some(pump) = payload.get("pump_status") {
                        if let Some(val) = pump.as_i64() {
                            info!("  🔌 Pump: {} ({})", val, match val {
                                1 => "ON",
                                0 => "OFF",
                                _ => "NO DATA"
                            });
                        }
                    }
                }

                if is_connected && data_found {
                    let body = json!(payload).to_string();
                    info!("📤 BRIDGE_TASK: Publishing payload: {}", body);
                    
                    if let Err(e) = mqtt_client
                        .publish("v1/devices/me/telemetry", QoS::AtLeastOnce, false, body)
                        .await
                    {
                        error!("❌ BRIDGE_TASK: MQTT publish failed: {:#?}", e);
                    } else {
                        info!("✅ BRIDGE_TASK: Data + Relay Status (Integer) published to ThingsBoard!");
                    }
                } else {
                    if !is_connected {
                        warn!("⚠️  BRIDGE_TASK: MQTT not connected, skipping publish.");
                    }
                    if !data_found {
                        warn!("⚠️  BRIDGE_TASK: No data to publish, skipping.");
                    }
                }
            }
            Err(e) => {
                error!("❌ BRIDGE_TASK: Failed to query InfluxDB: {:#?}", e);
            }
        }

        info!("⏳ BRIDGE_TASK: Waiting {} seconds until next query...", query_interval);
        info!("========================================");
        tokio::time::sleep(Duration::from_secs(query_interval)).await;
    }
}
