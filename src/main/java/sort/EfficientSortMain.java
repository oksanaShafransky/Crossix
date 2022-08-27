package sort;

import org.apache.log4j.Logger;

public class EfficientSortMain {
    static public DataLoader dataLoader = new DataLoader();

    public static void main(String[] args) {
        System.out.println("EfficientSort started!");
        //load chunks of data, sort and write to the disk
        String inputPath = CrossixConstants.INPUT_FILE_PATH + "/input_data_small.csv";
        dataLoader.loadCSVAndSplitData(inputPath);
        //write the list of lines out to the output stream, append new lines after each line.
        String chunkPath = CrossixConstants.CHUNKS_PATH;
        dataLoader.readChunksAndMergeSortWrite(chunkPath);
        System.out.println("EfficientSort finished successfully!");
    }

}
