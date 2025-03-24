import pyshark
import csv
import os
import glob
import multiprocessing
import pandas as pd


def pcap_to_csv(pcap_file, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename = os.path.basename(pcap_file)
    csv_filename = os.path.splitext(filename)[0] + '.csv'
    output_csv = os.path.join(output_dir, csv_filename)
    csv_headers = ['Timestamp', 'Source IP', 'Destination IP', 'Protocol', 'Info']

    try:
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(csv_headers)
            cap = pyshark.FileCapture(pcap_file)
            for packet in cap:
                try:
                    timestamp = packet.sniff_time
                    source_ip = packet.ip.src if hasattr(packet, 'ip') else 'N/A'
                    dest_ip = packet.ip.dst if hasattr(packet, 'ip') else 'N/A'
                    protocol = packet.highest_layer
                    info = str(packet)
                    row = [timestamp, source_ip, dest_ip, protocol, info]
                    csv_writer.writerow(row)
                except AttributeError as e:
                    print(f"Error processing packet (AttributeError): {e}")
                except Exception as e:
                    print(f"Error processing packet: {e}")
            cap.close()
        print(f"{output_csv} saved.")
    except Exception as e:
        print(f"Error processing file {pcap_file}: {e}")

def convertPcapToCsv(pcap_dir, csv_dir):
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    pcap_files = glob.glob(os.path.join(pcap_dir, '**/*.pcap'), recursive=True)
    with multiprocessing.Pool() as pool:
        pool.starmap(pcap_to_csv, [(pcap_file, csv_dir) for pcap_file in pcap_files])
    print("Complete.")



def allCsvToPickle(csv_dir, pickle_dir):
    if not os.path.exists(csv_dir):
        print("directory does not exist.")
    csv_files = glob.glob(os.path.join(csv_dir, '**/*.csv'), recursive=True)
    all_df=[]

    print("converting csv files to pickle")
    for csv_file in csv_files:
        df=pd.read_csv(csv_file)
        all_df.append(df)

        print(f"append {csv_file}")

    df_merged = pd.concat(all_df, axis=0, ignore_index=True)
    df_merged.to_pickle(pickle_dir+r'/botnetData.pkl')
    print("complete")
    return df_merged

def mergeCsvFiles(csvs_dir, csv_dir):
    if not os.path.exists(csvs_dir):
        print("directory does not exist.")
    csv_files = glob.glob(os.path.join(csvs_dir, '**/*.csv'), recursive=True)
    all_df=[]

    print("converting csv files to one csv")
    for csv_file in csv_files:
        df=pd.read_csv(csv_file)
        all_df.append(df)
        print(f"append {csv_file}")

    print("Dataframe merging")
    df_merged = pd.concat(all_df, axis=0, ignore_index=True)
    df_merged.to_csv(csv_dir+r'/botnetData.csv')
    print("complete")

