import pyshark
import csv
import os
import glob

input_path = r'CTU-13-Dataset\CTU-13-Dataset\*\*.pcap'
output_folder = 'dataset'

os.makedirs(output_folder, exist_ok=True)

pcap_files = glob.glob(input_path)


def process_pcap_to_csv(pcap_file, output_file):
    try:
        print(f"processing : {pcap_file}")
        capture = pyshark.FileCapture(pcap_file)

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Timestamp', 'Source IP', 'Destination IP', 'Protocol', 'Info'])

            for packet in capture:
                try:
                    timestamp = packet.sniff_time
                    source_ip = packet.ip.src if hasattr(packet, 'ip') else 'N/A'
                    dest_ip = packet.ip.dst if hasattr(packet, 'ip') else 'N/A'
                    protocol = packet.highest_layer
                    info = str(packet)

                    writer.writerow([timestamp, source_ip, dest_ip, protocol, info])
                except AttributeError:
                    continue
                except Exception as e:
                    print(f"error : {e}")
                    continue

        print(f"{output_file} created")

    except Exception as e:
        print(f"Error: {pcap_file} - {e}")

    finally:
        try:
            capture.close()
        except Exception as e:
            print(e)


# for pcap_file in pcap_files:
try:
    n=13
    filename = os.path.basename(pcap_files[n]).replace('.pcap', '.csv')
    output_file = os.path.join(output_folder, filename)
    process_pcap_to_csv(pcap_files[n], output_file)
except Exception as e:
    print(f"{pcap_files[n]} error occurred while processing the file: {e}")
