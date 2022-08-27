package com.sort.test;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import sort.CrossixConstants;
import sort.DataLoader;
import sort.InputData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class SortUnitTest {
    private static final String inputPath = CrossixConstants.INPUT_FILE_PATH + "/input_data_small.csv";
    private static final String outputPath = CrossixConstants.OUTPUT_FILE_PATH;
    private static final String chunkPath = CrossixConstants.CHUNKS_PATH;
    private static DataLoader dataLoader;

    @BeforeAll
    static void beforeAll(){
        dataLoader = new DataLoader();
    }

    @DisplayName("test number of chunks")
    @Test
    void testNumOfShunks(){
        dataLoader.loadCSVAndSplitData(inputPath);
        File dir = new File(chunkPath);
        assertTrue(dir.listFiles().length==5);
    }

    @DisplayName("test number of lines in output")
    @Test
    void testOutput() throws IOException {
        dataLoader.readChunksAndMergeSortWrite(chunkPath);
        File file = new File(outputPath + "/output_file.csv");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        int countLines = 0;
        while(reader.readLine()!=null) {
            countLines++;
            reader.readLine();
        }
        assertTrue(countLines>0);
    }
}
