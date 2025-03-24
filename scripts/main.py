import FileConverted as fileConverted
import paths as paths
if __name__ == '__main__':
    pcap_dir =paths.RAW_DATA_DIR+ r'\CTU-13-Dataset'
    csv_dir = paths.RAW_DATA_DIR + r'\CTU-13-Csv'
    # fileConverted.convertPcapToCsv(pcap_dir=pcap_dir,csv_dir=csv_dir)

    pickle_dir=paths.RAW_DATA_DIR
    # fileConverted.allCsvToPickle(csv_dir, pickle_dir)
    fileConverted.mergeCsvFiles(csv_dir, pickle_dir)