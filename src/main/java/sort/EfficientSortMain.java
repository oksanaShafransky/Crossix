package sort;

import org.apache.log4j.Logger;

public class EfficientSortMain {
    static public DataLoader dataLoader = new DataLoader();

    public static void main(String[] args) {
        System.out.println("EfficientSort started!");
        //load chunks of data, sort and write to the disk
        dataLoader.loadCSVAnSplitData();
        //write the list of lines out to the output stream, append new lines after each line.
        dataLoader.mergeChunksAndWriteToOutput();
        System.out.println("EfficientSort finished successfully!");
    }






}
