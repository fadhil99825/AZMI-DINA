#!/usr/bin/env python3
"""
DWSIM to InfluxDB Connector - Real-time Version
Mengambil nilai real-time dari DWSIM simulation menggunakan beberapa metode:
1. COM Interface (Windows) - akses langsung ke simulasi yang berjalan
2. File monitoring - monitoring file export CSV/TXT dari DWSIM
3. XML parsing (fallback) - untuk data statis
"""

import time
import sys
import xml.etree.ElementTree as ET
import os
import zipfile
import tempfile
import json
import csv
import threading
from pathlib import Path

# Import watchdog with error handling
try:
    # Add user site-packages to path if needed (for watchdog)
    user_site = os.path.expanduser('~/.local/lib/python3.10/site-packages')
    if user_site not in sys.path:
        sys.path.insert(0, user_site)

    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False
    # Create dummy classes for compatibility
    class FileSystemEventHandler:
        pass
    class Observer:
        pass

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_ORG = "ITS"
INFLUXDB_BUCKET = "DWSIM_DATA"
INFLUXDB_TOKEN = "lw9pwg_GymcNk2gOG9Wy2lzIFfHVBv6wVEBHJXhwIiL9LaCWm5IuY7MjOkXreOE_7e9Ujnl3TAq6Do3yCcOHYA=="

# Default DWSIM XML file path
DWSIM_XML_FILE = "/home/green/Documents/SKT/dwsim.dwxmz"

try:
    # Add user site-packages to path if needed
    import sys
    user_site = os.path.expanduser('~/.local/lib/python3.10/site-packages')
    if user_site not in sys.path:
        sys.path.insert(0, user_site)

    from influxdb_client import InfluxDBClient, Point, WritePrecision
    INFLUXDB_AVAILABLE = True
    print("InfluxDB client library loaded successfully")
except ImportError as e:
    print(f"InfluxDB client library not installed: {e}")
    print("Try: pip install --user influxdb-client")
    INFLUXDB_AVAILABLE = False

# Try to import COM libraries for Windows DWSIM interface
try:
    import win32com.client
    import pythoncom
    COM_AVAILABLE = True
    print("Windows COM interface available")
except ImportError:
    COM_AVAILABLE = False
    print("Windows COM interface not available (running on Linux/Mac)")

# Report watchdog availability
if WATCHDOG_AVAILABLE:
    print("File monitoring available")
else:
    print("Watchdog not available - file monitoring disabled")
    print("Try: pip install --user watchdog")


class DWSIMRealTimeConnector:
    """Class untuk koneksi real-time ke DWSIM simulation"""

    def __init__(self):
        self.dwsim_app = None
        self.simulation = None
        self.connected = False

    def connect_to_dwsim(self):
        """Connect ke DWSIM application yang sedang berjalan (Windows only)"""
        if not COM_AVAILABLE:
            print("COM interface not available - running on non-Windows system")
            return False

        try:
            # Initialize COM
            pythoncom.CoInitialize()

            # Connect to running DWSIM instance
            self.dwsim_app = win32com.client.GetActiveObject("DWSIM.Application")
            print("✓ Connected to running DWSIM application")

            # Get active simulation
            if self.dwsim_app.Simulations.Count > 0:
                self.simulation = self.dwsim_app.Simulations[0]
                print(f"✓ Found simulation: {self.simulation.Name}")
                self.connected = True
                return True
            else:
                print("✗ No active simulation found in DWSIM")
                return False

        except Exception as e:
            print(f"✗ Failed to connect to DWSIM: {e}")
            print("Make sure DWSIM is running with an open simulation")
            return False

    def get_stream_value(self, stream_name, property_name):
        """Get nilai property dari stream tertentu"""
        if not self.connected:
            return None

        try:
            # Find the stream object
            stream = None
            for obj in self.simulation.SimulationObjects.Values:
                if hasattr(obj, 'Tag') and obj.Tag == stream_name:
                    stream = obj
                    break

            if stream is None:
                print(f"Stream '{stream_name}' not found")
                return None

            # Get property value
            if hasattr(stream, property_name):
                return getattr(stream, property_name)
            else:
                # Try alternative property access
                try:
                    return stream.GetPropertyValue(property_name)
                except:
                    print(f"Property '{property_name}' not found on stream '{stream_name}'")
                    return None

        except Exception as e:
            print(f"Error getting stream value: {e}")
            return None

    def get_water_i_values_realtime(self):
        """Get real-time values dari Water_i stream"""
        if not self.connected:
            return None

        try:
            values = {}

            # Get temperature (convert K to C)
            temp_k = self.get_stream_value("Water_i", "Temperature")
            if temp_k is not None:
                values['temperature_celsius'] = temp_k - 273.15

            # Get pressure (convert Pa to bar)
            pressure_pa = self.get_stream_value("Water_i", "Pressure")
            if pressure_pa is not None:
                values['pressure_bar'] = pressure_pa / 100000

            # Get mass flow (kg/h to kg/s)
            massflow_kgh = self.get_stream_value("Water_i", "MassFlow")
            if massflow_kgh is not None:
                values['mass_flow_kg_s'] = massflow_kgh / 3600

            # Get other properties
            density = self.get_stream_value("Water_i", "Density")
            if density is not None:
                values['density_kg_m3'] = density

            enthalpy = self.get_stream_value("Water_i", "SpecificEnthalpy")
            if enthalpy is not None:
                values['enthalpy_kj_kg'] = enthalpy

            return values if values else None

        except Exception as e:
            print(f"Error getting real-time values: {e}")
            return None

    def disconnect(self):
        """Disconnect dari DWSIM"""
        if COM_AVAILABLE:
            try:
                pythoncom.CoUninitialize()
                self.connected = False
                print("✓ Disconnected from DWSIM")
            except:
                pass


class DWSIMFileMonitor(FileSystemEventHandler):
    """Monitor untuk file export DWSIM (CSV, TXT, JSON)"""

    def __init__(self, callback_function, target_files=None):
        self.callback = callback_function
        self.target_files = target_files or ['water_i_data.csv', 'stream_data.txt', 'dwsim_export.json']
        self.last_modified = {}

    def on_modified(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        filename = file_path.name.lower()

        # Check if this is a file we're monitoring
        if any(target in filename for target in self.target_files):
            # Avoid multiple events for same file
            current_time = time.time()
            if filename in self.last_modified:
                if current_time - self.last_modified[filename] < 2:  # 2 second cooldown
                    return

            self.last_modified[filename] = current_time
            print(f"📁 File change detected: {filename}")

            # Call callback function with file path
            try:
                self.callback(str(file_path))
            except Exception as e:
                print(f"Error processing file change: {e}")


class DWSIMFileReader:
    """Read data dari file export DWSIM"""

    @staticmethod
    def read_csv_export(file_path, stream_name="Water_i"):
        """Read data dari CSV export DWSIM"""
        try:
            values = {}
            with open(file_path, 'r') as csvfile:
                # Try different CSV formats
                sample = csvfile.read(1024)
                csvfile.seek(0)

                # Detect delimiter
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter

                reader = csv.DictReader(csvfile, delimiter=delimiter)

                # Find row for our stream
                for row in reader:
                    if stream_name.lower() in str(row).lower():
                        # Extract common properties
                        for key, value in row.items():
                            key_lower = key.lower()
                            if 'temp' in key_lower:
                                values['temperature_celsius'] = float(value)
                            elif 'pressure' in key_lower:
                                values['pressure_bar'] = float(value)
                            elif 'mass' in key_lower and 'flow' in key_lower:
                                values['mass_flow_kg_s'] = float(value)
                            elif 'density' in key_lower:
                                values['density_kg_m3'] = float(value)
                        break

            return values if values else None

        except Exception as e:
            print(f"Error reading CSV: {e}")
            return None

    @staticmethod
    def read_json_export(file_path, stream_name="Water_i"):
        """Read data dari JSON export DWSIM"""
        try:
            with open(file_path, 'r') as jsonfile:
                data = json.load(jsonfile)

            # Navigate JSON structure to find stream data
            if 'streams' in data and stream_name in data['streams']:
                stream_data = data['streams'][stream_name]

                values = {}
                # Map JSON fields to our standard format
                if 'temperature' in stream_data:
                    values['temperature_celsius'] = stream_data['temperature']
                if 'pressure' in stream_data:
                    values['pressure_bar'] = stream_data['pressure']
                if 'mass_flow' in stream_data:
                    values['mass_flow_kg_s'] = stream_data['mass_flow']

                return values if values else None

        except Exception as e:
            print(f"Error reading JSON: {e}")
            return None

    @staticmethod
    def read_txt_export(file_path, stream_name="Water_i"):
        """Read data dari TXT export DWSIM (format bebas)"""
        try:
            values = {}
            with open(file_path, 'r') as txtfile:
                content = txtfile.read()

            # Simple pattern matching for common formats
            import re

            # Look for temperature
            temp_match = re.search(rf'{stream_name}.*?temperature[:\s=]+(\d+\.?\d*)', content, re.IGNORECASE)
            if temp_match:
                values['temperature_celsius'] = float(temp_match.group(1))

            # Look for pressure
            pressure_match = re.search(rf'{stream_name}.*?pressure[:\s=]+(\d+\.?\d*)', content, re.IGNORECASE)
            if pressure_match:
                values['pressure_bar'] = float(pressure_match.group(1))

            return values if values else None

        except Exception as e:
            print(f"Error reading TXT: {e}")
            return None


class DWSIMXMLParser:
    """Class untuk parsing file XML DWSIM dan mengambil data stream (fallback method)"""

    def __init__(self, xml_file_path=None):
        self.xml_file_path = xml_file_path or DWSIM_XML_FILE
        self.root = None

    def load_xml_file(self):
        """Load dan parse file XML DWSIM (handle ZIP dan XML format)"""
        try:
            if not os.path.exists(self.xml_file_path):
                print(f"File tidak ditemukan: {self.xml_file_path}")
                return False

            # Check if file is ZIP (DWSIM .dwxmz format)
            if zipfile.is_zipfile(self.xml_file_path):
                print("Detected ZIP file (DWSIM .dwxmz format)")
                return self._load_from_zip()
            else:
                print("Detected XML file")
                return self._load_from_xml()

        except Exception as e:
            print(f"Error loading file: {e}")
            return False

    def _load_from_zip(self):
        """Load XML from ZIP file"""
        try:
            with zipfile.ZipFile(self.xml_file_path, 'r') as zip_ref:
                # List contents to find XML file
                file_list = zip_ref.namelist()
                print(f"ZIP contains {len(file_list)} files")

                # Look for the main simulation file
                xml_file = None
                for filename in file_list:
                    if filename.lower().endswith('.xml') or filename.endswith('.dwsim'):
                        xml_file = filename
                        break

                if not xml_file:
                    # Try to find any file that might contain simulation data
                    for filename in file_list:
                        if not filename.endswith('/'):  # Not a directory
                            xml_file = filename
                            break

                if not xml_file:
                    print("No XML file found in ZIP archive")
                    return False

                print(f"Reading {xml_file} from ZIP")

                # Extract and parse XML content
                with zip_ref.open(xml_file) as file:
                    content = file.read()

                # Handle both string and bytes content
                if isinstance(content, bytes):
                    content = content.decode('utf-8')

                # Parse XML
                self.root = ET.fromstring(content)
                print("XML content parsed successfully from ZIP")
                return True

        except Exception as e:
            print(f"Error loading from ZIP: {e}")
            return False

    def _load_from_xml(self):
        """Load XML from regular XML file"""
        try:
            tree = ET.parse(self.xml_file_path)
            self.root = tree.getroot()
            print(f"XML file loaded successfully: {self.xml_file_path}")
            return True
        except Exception as e:
            print(f"Error loading XML: {e}")
            return False

    def find_water_i_stream(self):
        """Cari stream Water_i dalam XML"""
        if self.root is None:
            print("XML not loaded")
            return None

        try:
            # Debug: print root tag
            print(f"Root element: {self.root.tag}")

            # Cari di SimulationObjects untuk stream dengan nama Water_i
            simulation_objects = self.root.find('SimulationObjects')
            if simulation_objects is None:
                print("SimulationObjects not found in XML")
                # Try to find all child elements for debugging
                print("Available root children:")
                for child in self.root:
                    print(f"  - {child.tag}")
                return None

            print(f"Found SimulationObjects with {len(simulation_objects)} children")

            # Debug: list all streams with more details
            material_streams = []
            for obj in simulation_objects.findall('SimulationObject'):
                obj_type = obj.find('Type')
                if obj_type is not None and 'MaterialStream' in obj_type.text:
                    tag_element = obj.find('Tag')
                    name_element = obj.find('Name')
                    comp_name_element = obj.find('ComponentName')
                    tag_name = tag_element.text if tag_element is not None else "No Tag"
                    name_name = name_element.text if name_element is not None else "No Name"
                    comp_name = comp_name_element.text if comp_name_element is not None else "No ComponentName"
                    material_streams.append(f"Tag: {tag_name}, Name: {name_name}, ComponentName: {comp_name}")

            print("Found Material Streams:")
            for stream in material_streams:
                print(f"  - {stream}")

            # Look for Water_i stream - try multiple criteria
            for obj in simulation_objects.findall('SimulationObject'):
                # Cek type apakah MaterialStream
                obj_type = obj.find('Type')
                if obj_type is not None and 'MaterialStream' in obj_type.text:

                    # Try different ways to identify Water_i stream
                    tag_element = obj.find('Tag')
                    name_element = obj.find('Name')
                    comp_name_element = obj.find('ComponentName')

                    # Check Tag first
                    if tag_element is not None and tag_element.text == 'Water_i':
                        print("Found Water_i stream (by Tag)")
                        return obj

                    # Check ComponentName
                    if comp_name_element is not None and 'Water_i' in comp_name_element.text:
                        print("Found Water_i stream (by ComponentName)")
                        return obj

                    # Check if name contains specific ID for Water_i (from your XML)
                    if name_element is not None and name_element.text == 'MAT-4e96a874-eb97-470b-8637-0b25e4554a94':
                        print("Found Water_i stream (by Name ID)")
                        return obj

            print("Water_i stream not found in XML")
            return None

        except Exception as e:
            print(f"Error finding Water_i stream: {e}")
            return None

    def get_water_i_values(self):
        """Extract nilai dari stream Water_i"""
        if self.root is None:
            if not self.load_xml_file():
                return None

        water_stream = self.find_water_i_stream()
        if water_stream is None:
            return None

        try:
            # Debug: Show stream structure
            print("Water_i stream structure:")
            for child in water_stream:
                print(f"  - {child.tag}")

            # Ambil data dari Phases[0] (Mixture phase)
            phases = water_stream.find('Phases')
            if phases is None:
                print("Phases not found in Water_i stream")
                return None

            print(f"Found Phases element with {len(phases)} phase(s)")

            # Debug: List all phases
            for i, phase in enumerate(phases.findall('Phase')):
                phase_id = phase.find('ID')
                phase_name = phase.find('ComponentName')
                id_text = phase_id.text if phase_id is not None else "No ID"
                name_text = phase_name.text if phase_name is not None else "No Name"
                print(f"  Phase {i}: ID={id_text}, Name={name_text}")

            # Ambil fase pertama (ID=0, Mixture)
            mixture_phase = None
            for phase in phases.findall('Phase'):
                phase_id = phase.find('ID')
                if phase_id is not None and phase_id.text == '0':
                    mixture_phase = phase
                    break

            if mixture_phase is None:
                print("Mixture phase (ID=0) not found")
                return None

            print("Found mixture phase")

            # Try different ways to find Properties
            properties = None

            # Method 1: Properties element
            properties = mixture_phase.find('Properties')

            # Method 2: Properties1 element (from your original XML)
            if properties is None or (properties is not None and properties.text and "BaseClasses" in properties.text):
                properties1 = mixture_phase.find('Properties1')
                if properties1 is not None:
                    print("Using Properties1 element")
                    properties = properties1

            # Method 3: Look for child with detailed properties
            if properties is None or (properties is not None and len(list(properties)) == 0):
                print("Looking for alternative properties structure...")
                print("Available elements in mixture phase:")
                for child in mixture_phase:
                    print(f"  - {child.tag}: {child.text[:50] if child.text else 'No text'}{'...' if child.text and len(child.text) > 50 else ''}")

                    # Try to find element with Type that contains Properties
                    if child.tag not in ['ID', 'ComponentDescription', 'ComponentName', 'Compounds', 'Name']:
                        type_elem = child.find('Type')
                        if type_elem is not None and 'Properties' in type_elem.text:
                            print(f"Found properties in {child.tag}")
                            properties = child
                            break

            if properties is None:
                print("Properties not found in any structure")
                return None

            print(f"Using properties element: {properties.tag}")

            # Debug: List available properties
            print("Available properties:")
            for prop in properties:
                print(f"  - {prop.tag}: {prop.text[:50] if prop.text else 'None'}{'...' if prop.text and len(prop.text) > 50 else ''}")

            # Check if properties contain Type element like in your original XML
            prop_type = properties.find('Type')
            if prop_type is not None:
                print(f"  Properties Type: {prop_type.text}")

            # Extract nilai-nilai property
            values = {}

            # Temperature (dari Kelvin ke Celsius)
            temp_element = properties.find('temperature')
            if temp_element is not None:
                temp_kelvin = float(temp_element.text)
                values['temperature_celsius'] = temp_kelvin - 273.15

            # Pressure (dari Pa ke bar)
            pressure_element = properties.find('pressure')
            if pressure_element is not None:
                pressure_pa = float(pressure_element.text)
                values['pressure_bar'] = pressure_pa / 100000

            # Mass flow (kg/h ke kg/s)
            massflow_element = properties.find('massflow')
            if massflow_element is not None:
                massflow_kg_h = float(massflow_element.text)
                values['mass_flow_kg_s'] = massflow_kg_h / 3600

            # Density
            density_element = properties.find('density')
            if density_element is not None:
                values['density_kg_m3'] = float(density_element.text)

            # Enthalpy
            enthalpy_element = properties.find('enthalpy')
            if enthalpy_element is not None:
                values['enthalpy_kj_kg'] = float(enthalpy_element.text)

            # Molar flow
            molarflow_element = properties.find('molarflow')
            if molarflow_element is not None:
                values['molar_flow_kmol_h'] = float(molarflow_element.text)

            # Volumetric flow
            volumetric_flow_element = properties.find('volumetric_flow')
            if volumetric_flow_element is not None:
                values['volumetric_flow_m3_h'] = float(volumetric_flow_element.text)

            print(f"Successfully extracted {len(values)} properties from Water_i")
            return values

        except Exception as e:
            print(f"Error extracting Water_i values: {e}")
            return None


class InfluxDBUploader:
    """Class untuk upload data ke InfluxDB"""

    def __init__(self, url, org, bucket, token):
        if not INFLUXDB_AVAILABLE:
            raise RuntimeError("InfluxDB client not available")

        self.url = url
        self.org = org
        self.bucket = bucket
        self.token = token
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api()

    def test_connection(self):
        """Test koneksi ke InfluxDB server"""
        try:
            print(f"Testing connection to InfluxDB at {self.url}...")

            # Test using query API to ping the server
            query_api = self.client.query_api()

            # Simple query to test connection
            query = f'from(bucket: "{self.bucket}") |> range(start: -1m) |> limit(n:1)'

            try:
                result = query_api.query(query=query, org=self.org)
                print("✓ InfluxDB connection successful!")
                print(f"✓ Organization: {self.org}")
                print(f"✓ Bucket: {self.bucket}")
                return True
            except Exception as query_error:
                # Even if query fails, connection might be OK, just bucket might not exist
                print(f"✓ InfluxDB server reachable at {self.url}")
                print(f"⚠ Query test failed (bucket might not exist): {query_error}")

                # Try to test with a simple health check via HTTP
                import requests
                health_url = f"{self.url}/health"
                response = requests.get(health_url, timeout=5)
                if response.status_code == 200:
                    print("✓ InfluxDB health check passed")
                    return True
                else:
                    print(f"✗ InfluxDB health check failed: {response.status_code}")
                    return False

        except Exception as e:
            print(f"✗ InfluxDB connection failed: {e}")
            print("Please check:")
            print(f"  - Server is running at {self.url}")
            print(f"  - Token is valid")
            print(f"  - Organization '{self.org}' exists")
            print(f"  - Bucket '{self.bucket}' exists")
            return False

    def upload_data(self, values, stream_name="Water_i", simulation_name="MySimulation"):
        if not values:
            print("No data to upload")
            return False

        try:
            points = []
            for key, val in values.items():
                point = (
                    Point("dwsim_measurement")
                    .tag("stream", stream_name)
                    .tag("simulation", simulation_name)
                    .field(key, float(val))
                    .time(time.time_ns(), WritePrecision.NS)
                )
                points.append(point)

            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
            print("Data uploaded successfully to InfluxDB")
            return True
        except Exception as e:
            print(f"Error uploading to InfluxDB: {e}")
            return False

    def upload_temperature_only(self, temperature_celsius, stream_name="Water_i", simulation_name="DWSIM_Simulation"):
        """Upload only temperature data to InfluxDB"""
        if temperature_celsius is None:
            print("No temperature data to upload")
            return False

        try:
            point = (
                Point("dwsim_temperature")
                .tag("stream", stream_name)
                .tag("simulation", simulation_name)
                .field("temperature_celsius", float(temperature_celsius))
                .time(time.time_ns(), WritePrecision.NS)
            )

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            print(f"Temperature data uploaded: {temperature_celsius:.2f} °C")
            return True
        except Exception as e:
            print(f"Error uploading temperature to InfluxDB: {e}")
            return False


def main():
    print("DWSIM XML to InfluxDB Connector")
    print("=" * 40)

    # Initialize XML parser
    xml_parser = DWSIMXMLParser(DWSIM_XML_FILE)

    # Load XML file
    if not xml_parser.load_xml_file():
        print("Failed to load DWSIM XML file")
        return

    # Extract Water_i values
    values = xml_parser.get_water_i_values()
    if not values:
        print("Failed to get Water_i values from XML")
        return

    print("\nWater_i Stream Values:")
    for param, value in values.items():
        print(f"  {param}: {value:.4f}")

    # Upload to InfluxDB
    if INFLUXDB_AVAILABLE:
        uploader = InfluxDBUploader(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN)
        uploader.upload_data(values, stream_name="Water_i", simulation_name="DWSIM_Simulation")
    else:
        print("InfluxDB client not available, skipping upload")


def continuous_monitoring(interval=15, xml_file_path=None):
    print(f"Starting continuous XML monitoring (interval: {interval} s)")
    print("Uploading to InfluxDB every {interval} seconds regardless of data changes")
    print("Press Ctrl+C to stop")

    xml_parser = DWSIMXMLParser(xml_file_path or DWSIM_XML_FILE)

    if not INFLUXDB_AVAILABLE:
        print("InfluxDB client not available")
        return

    uploader = InfluxDBUploader(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN)

    # Test connection first
    print("Testing InfluxDB connection...")
    if not uploader.test_connection():
        print("❌ Cannot connect to InfluxDB. Please check configuration.")
        return

    print("✅ InfluxDB connection successful!")

    # Load XML initially to get initial values
    last_values = None
    if xml_parser.load_xml_file():
        last_values = xml_parser.get_water_i_values()
        if last_values:
            print("Initial data loaded from XML successfully")
        else:
            print("⚠️ Could not load initial data from XML")

    cycle_count = 0

    try:
        while True:
            cycle_count += 1
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n[Cycle #{cycle_count:03d}] {timestamp} - Processing...")

            # Always try to read XML file
            current_values = None
            if xml_parser.load_xml_file():
                current_values = xml_parser.get_water_i_values()
                if current_values:
                    last_values = current_values  # Update last known good values
                    print("✓ Fresh data loaded from XML")
                else:
                    print("⚠️ No data in XML, using last known values")
            else:
                print("⚠️ Failed to load XML, using last known values")

            # Always upload data (current or last known values)
            values_to_upload = current_values or last_values

            if values_to_upload:
                print("📤 Uploading data to InfluxDB...")

                # Upload full data first
                success = uploader.upload_data(values_to_upload,
                                             stream_name="Water_i",
                                             simulation_name="DWSIM_Simulation")

                # Also upload temperature-only for backend compatibility
                if 'temperature_celsius' in values_to_upload:
                    temp_success = uploader.upload_temperature_only(
                        values_to_upload['temperature_celsius'],
                        stream_name="Water_i",
                        simulation_name="DWSIM_Simulation"
                    )
                    success = success and temp_success

                if success:
                    print("✅ Data uploaded successfully!")
                    # Show current values
                    print(f"  📊 Temperature: {values_to_upload.get('temperature_celsius', 'N/A'):.2f} °C")
                    print(f"  📊 Pressure: {values_to_upload.get('pressure_bar', 'N/A'):.2f} bar")
                    print(f"  📊 Mass Flow: {values_to_upload.get('mass_flow_kg_s', 'N/A'):.4f} kg/s")
                else:
                    print("❌ Upload failed!")
            else:
                print("❌ No data available to upload")

            print(f"⏱️ Waiting {interval} seconds until next upload...")
            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n🛑 Monitoring stopped after {cycle_count} cycles")


def realtime_monitoring(interval=15, method="auto", export_folder=None, stream_name="Water_i"):
    """Real-time monitoring dengan multiple methods"""
    print(f"🚀 Starting Real-time DWSIM Monitoring (interval: {interval} s)")
    print("=" * 60)
    print("Available methods:")
    print("  1. COM Interface - Direct connection to running DWSIM (Windows only)")
    print("  2. File monitoring - Monitor export files (CSV/JSON/TXT)")
    print("  3. XML fallback - Traditional XML parsing")
    print("=" * 60)

    if not INFLUXDB_AVAILABLE:
        print("❌ InfluxDB client not available")
        return

    # Initialize uploader and test connection
    uploader = InfluxDBUploader(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN)

    print("\n🔧 Testing InfluxDB connection...")
    if not uploader.test_connection():
        print("❌ Cannot connect to InfluxDB. Please fix connection issues before continuing.")
        return

    print("✅ InfluxDB connection successful!")

    # Determine monitoring method
    data_source = None
    monitoring_method = None

    if method == "auto" or method == "com":
        # Try COM interface first (Windows only)
        if COM_AVAILABLE:
            print("\n🔗 Attempting COM interface connection...")
            com_connector = DWSIMRealTimeConnector()
            if com_connector.connect_to_dwsim():
                data_source = com_connector
                monitoring_method = "COM Interface"
                print("✅ Using COM Interface for real-time data")
            else:
                print("⚠️ COM interface failed, trying file monitoring...")

    if data_source is None and (method == "auto" or method == "file"):
        # Try file monitoring
        if WATCHDOG_AVAILABLE:
            print("\n📁 Setting up file monitoring...")
            export_path = export_folder or os.path.dirname(DWSIM_XML_FILE)

            # Create export folder if needed
            os.makedirs(export_path, exist_ok=True)

            print(f"📂 Monitoring folder: {export_path}")
            print("📝 Supported files: CSV, JSON, TXT exports from DWSIM")
            print("   Example: water_i_data.csv, stream_data.txt, dwsim_export.json")

            monitoring_method = "File Monitoring"
            print("✅ File monitoring setup complete")
        else:
            print("⚠️ File monitoring not available, falling back to XML...")

    if data_source is None:
        # Fallback to XML monitoring
        print("\n📄 Using XML fallback method...")
        xml_parser = DWSIMXMLParser(DWSIM_XML_FILE)
        data_source = xml_parser
        monitoring_method = "XML Fallback"
        print("✅ XML parser initialized")

    print(f"\n🎯 Active monitoring method: {monitoring_method}")
    print("Press Ctrl+C to stop\n")

    # Start monitoring based on method
    try:
        if monitoring_method == "COM Interface":
            _monitor_with_com(data_source, uploader, interval, stream_name)
        elif monitoring_method == "File Monitoring":
            _monitor_with_files(export_path, uploader, interval, stream_name)
        else:
            _monitor_with_xml(data_source, uploader, interval, stream_name)

    except KeyboardInterrupt:
        print(f"\n🛑 Real-time monitoring stopped")
        if hasattr(data_source, 'disconnect'):
            data_source.disconnect()


def _monitor_with_com(com_connector, uploader, interval, stream_name):
    """Monitor using COM interface"""
    cycle_count = 0
    while True:
        cycle_count += 1
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"[COM #{cycle_count:03d}] {timestamp} - Reading from DWSIM...")

        values = com_connector.get_water_i_values_realtime()
        if values:
            print(f"  📊 Real-time data: T={values.get('temperature_celsius', 'N/A'):.2f}°C, "
                  f"P={values.get('pressure_bar', 'N/A'):.2f}bar")

            if uploader.upload_data(values, stream_name=stream_name, simulation_name="DWSIM_RealTime"):
                print("  ✅ Data uploaded successfully!")
            else:
                print("  ❌ Upload failed")
        else:
            print("  ⚠️ No data available")

        time.sleep(interval)


def _monitor_with_files(folder_path, uploader, interval, stream_name):
    """Monitor using file changes"""
    last_data = {}

    def process_file_change(file_path):
        nonlocal last_data
        print(f"  📁 Processing: {os.path.basename(file_path)}")

        values = None
        file_ext = Path(file_path).suffix.lower()

        if file_ext == '.csv':
            values = DWSIMFileReader.read_csv_export(file_path, stream_name)
        elif file_ext == '.json':
            values = DWSIMFileReader.read_json_export(file_path, stream_name)
        elif file_ext == '.txt':
            values = DWSIMFileReader.read_txt_export(file_path, stream_name)

        if values:
            # Check if data changed
            if values != last_data:
                print(f"  📊 New data: T={values.get('temperature_celsius', 'N/A'):.2f}°C")
                if uploader.upload_data(values, stream_name=stream_name, simulation_name="DWSIM_FileMonitor"):
                    print("  ✅ Data uploaded!")
                    last_data = values.copy()
                else:
                    print("  ❌ Upload failed")
            else:
                print("  ↔️ No change in data")

    # Setup file monitoring
    event_handler = DWSIMFileMonitor(process_file_change)
    observer = Observer()
    observer.schedule(event_handler, folder_path, recursive=False)
    observer.start()

    print(f"📂 File monitoring active on: {folder_path}")
    print("💡 To generate exports in DWSIM:")
    print("   - Use Reports > Export > Stream Data")
    print("   - Save as CSV/JSON/TXT in the monitored folder")

    try:
        while True:
            time.sleep(interval)
            print(f"⏰ {time.strftime('%H:%M:%S')} - File monitoring active...")
    finally:
        observer.stop()
        observer.join()


def _monitor_with_xml(xml_parser, uploader, interval, stream_name):
    """Monitor using XML parsing (fallback)"""
    cycle_count = 0
    last_modified = 0

    while True:
        cycle_count += 1
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        # Check if XML file was modified
        try:
            current_modified = os.path.getmtime(xml_parser.xml_file_path)
            if current_modified > last_modified:
                print(f"[XML #{cycle_count:03d}] {timestamp} - XML file updated, reading...")
                last_modified = current_modified

                if xml_parser.load_xml_file():
                    values = xml_parser.get_water_i_values()
                    if values:
                        print(f"  📊 Data: T={values.get('temperature_celsius', 'N/A'):.2f}°C")
                        if uploader.upload_data(values, stream_name=stream_name, simulation_name="DWSIM_XML"):
                            print("  ✅ Data uploaded!")
                        else:
                            print("  ❌ Upload failed")
                    else:
                        print("  ⚠️ No data extracted")
                else:
                    print("  ❌ Failed to load XML")
            else:
                print(f"[XML #{cycle_count:03d}] {timestamp} - No file changes")

        except Exception as e:
            print(f"  ❌ Error checking file: {e}")

        time.sleep(interval)


def continuous_temperature_monitoring(interval=15, xml_file_path=None):
    """Backward compatibility function - now uses realtime_monitoring"""
    print("⚠️ Using legacy function - consider using 'realtime_monitoring' instead")
    realtime_monitoring(interval=interval, method="xml", stream_name="Water_i")


def test_influxdb_connection():
    """Test InfluxDB connection only"""
    print("InfluxDB Connection Test")
    print("=" * 30)

    if not INFLUXDB_AVAILABLE:
        print("❌ InfluxDB client library not available")
        print("Please install: pip install --user influxdb-client")
        return

    uploader = InfluxDBUploader(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN)
    success = uploader.test_connection()

    if success:
        print("\n🎉 All connection tests passed!")
        print("Your DWSIM temperature monitoring should work correctly.")
    else:
        print("\n❌ Connection test failed!")
        print("Please fix the connection issues before running temperature monitoring.")


def show_help():
    """Show help information"""
    print("DWSIM Real-time Connector - Usage Guide")
    print("=" * 50)
    print("\nCommands:")
    print("  python dwsim.py                     # Single XML read and upload")
    print("  python dwsim.py test                # Test InfluxDB connection")
    print("  python dwsim.py realtime [options]  # Real-time monitoring (NEW!)")
    print("  python dwsim.py continuous [int]    # Legacy continuous monitoring")
    print("  python dwsim.py temperature [int]   # Legacy temperature monitoring")
    print("\nReal-time Options:")
    print("  python dwsim.py realtime             # Auto-detect best method")
    print("  python dwsim.py realtime com         # Force COM interface (Windows)")
    print("  python dwsim.py realtime file        # Force file monitoring")
    print("  python dwsim.py realtime xml         # Force XML monitoring")
    print("  python dwsim.py realtime --interval 10      # Set interval to 10s")
    print("  python dwsim.py realtime --folder /path/    # Set export folder")
    print("\nMethods:")
    print("  1. COM Interface (Windows only):")
    print("     - Direct connection to running DWSIM")
    print("     - Real-time data from simulation")
    print("     - Best performance and accuracy")
    print("\n  2. File Monitoring (All platforms):")
    print("     - Monitor CSV/JSON/TXT exports from DWSIM")
    print("     - Setup export in DWSIM: Reports > Export > Stream Data")
    print("     - Save exports to monitored folder")
    print("\n  3. XML Fallback (All platforms):")
    print("     - Traditional XML file monitoring")
    print("     - Only detects file save changes")
    print("     - Least real-time but most compatible")
    print("\nExamples:")
    print("  python dwsim.py realtime --interval 5 com")
    print("  python dwsim.py realtime --folder /tmp/dwsim file")


def parse_realtime_args():
    """Parse command line arguments for realtime monitoring"""
    args = {
        'method': 'auto',
        'interval': 15,
        'folder': None,
        'stream': 'Water_i'
    }

    i = 2  # Start after 'realtime'
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--interval' and i + 1 < len(sys.argv):
            args['interval'] = int(sys.argv[i + 1])
            i += 2
        elif arg == '--folder' and i + 1 < len(sys.argv):
            args['folder'] = sys.argv[i + 1]
            i += 2
        elif arg == '--stream' and i + 1 < len(sys.argv):
            args['stream'] = sys.argv[i + 1]
            i += 2
        elif arg in ['auto', 'com', 'file', 'xml']:
            args['method'] = arg
            i += 1
        else:
            i += 1

    return args


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "help":
        show_help()
    elif len(sys.argv) > 1 and sys.argv[1] == "test":
        test_influxdb_connection()
    elif len(sys.argv) > 1 and sys.argv[1] == "realtime":
        # New real-time monitoring with options
        args = parse_realtime_args()
        print(f"🎯 Starting real-time monitoring...")
        print(f"   Method: {args['method']}")
        print(f"   Interval: {args['interval']}s")
        print(f"   Stream: {args['stream']}")
        if args['folder']:
            print(f"   Export folder: {args['folder']}")
        realtime_monitoring(
            interval=args['interval'],
            method=args['method'],
            export_folder=args['folder'],
            stream_name=args['stream']
        )
    elif len(sys.argv) > 1 and sys.argv[1] == "continuous":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 15  # Default 15 seconds
        xml_file = sys.argv[3] if len(sys.argv) > 3 else None
        continuous_monitoring(interval, xml_file)
    elif len(sys.argv) > 1 and sys.argv[1] == "temperature":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 15
        xml_file = sys.argv[3] if len(sys.argv) > 3 else None
        continuous_temperature_monitoring(interval, xml_file)
    else:
        xml_file = sys.argv[1] if len(sys.argv) > 1 else None
        if xml_file:
            # Override default file path
            parser = DWSIMXMLParser(xml_file)
            values = parser.get_water_i_values()
            if values:
                print("\nWater_i Stream Values:")
                for param, value in values.items():
                    print(f"  {param}: {value:.4f}")
                if INFLUXDB_AVAILABLE:
                    uploader = InfluxDBUploader(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN)
                    uploader.upload_data(values, stream_name="Water_i", simulation_name="DWSIM_Simulation")
            else:
                print("Failed to extract data from specified XML file")
        else:
            main()
