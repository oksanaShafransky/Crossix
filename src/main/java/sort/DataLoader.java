package sort;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

/**
 * This class receives a big file, splits it to several temporary sorted files according to the defined CHUNK_SIZE,
 * and then merges those files in sorted manner to the final output.
 */
public class DataLoader {
    private static Logger logger = Logger.getLogger(DataLoader.class.toString());

    /**
     * Reads the input file and splits it into sorted chunks which are written to temporary files.
     */
    public void loadCSVAnSplitData() {
        String inputPath = CrossixConstants.INPUT_FILE_PATH + "/input_data_small.csv";
        FileInputStream inputStream = null;
        Scanner sc = null;
        int line_counter = 0;
        int chunk_counter = 0;

        try {
            inputStream = new FileInputStream(inputPath);
            sc = new Scanner(inputStream, "UTF-8");
            List<InputData> tempList = new ArrayList<>();
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                line_counter++;
                tempList.add(prepareInputData(line.split(",")));
                if (line_counter % CrossixConstants.CHUNK_SIZE == 0) {
                    chunk_counter++;
                    sortInputDataList(tempList);
                    writeInputDataChunk(tempList, chunk_counter);
                    tempList.clear();
                    System.out.println("writing chunk #" + chunk_counter);
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("loadCSVAnSplitData failed", e);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (sc != null) {
                    sc.close();
                }
            } catch (IOException e){
                logger.error("IOException on finally", e);
            }
        }
        logger.info(String.format("All data was loaded and splited to %s chunks of %s lines successfully.", chunk_counter, line_counter));
    }


    /**
     * This method prepares single InputData object from the line we read on the csv file.
     * @param values
     * @return InputData or null if error
     */
    private InputData prepareInputData(String[] values) {
        //handle erroneous data (expected 4 strings)
        if(values.length != 4){
            logger.error("prepareInputData got invalid page view");
            return null;
        }
        //valid data
        try {
            InputData inputData = new InputData();
            inputData.setVisitorId(values[0]);
            inputData.setSiteUrl(values[1]);
            inputData.setPageViewUrl(values[2]);
            inputData.setTimestamp(Long.valueOf(values[3]));
            return inputData;
        } catch (Exception e){
            logger.error("prepareInputData catch exception.", e);
            return null;
        }
    }

    /**
     * This method write the chunk to the file with counter
     * @param dataInputList
     * @param counter
     */
    private void writeInputDataChunk(List<InputData> dataInputList, int counter) {
        try {
            PrintWriter writer = new PrintWriter(CrossixConstants.CHUNKS_PATH + "/file_" + counter);
            for (InputData inputData : dataInputList) {
                writer.println(inputData.toString());
            }
            writer.close();
        } catch (IOException e){
            logger.error("Error on writeInputDataList", e);
        }
    }

    public void sortInputDataList(List<InputData> inputDataList) {
        if(inputDataList!=null && !inputDataList.isEmpty()) {
            Collections.sort(inputDataList, new Comparator<InputData>() {
                @Override
                public int compare(InputData o1, InputData o2) {
                    return o1.getVisitorId().compareTo(o2.getVisitorId());
                }
            });
        }
    }

    /**
     * Writes the list of lines out to the output stream, append new lines after each line.
     */
    public void mergeChunksAndWriteToOutput() {
        File dir = new File(CrossixConstants.CHUNKS_PATH);
        File[] listOfFiles = dir.listFiles();
        List<BufferedReader> readers = new ArrayList<>();
        Map<InputData, BufferedReader> map = new HashMap<>();
        PrintWriter writer = null;

        try {
            writer = new PrintWriter(CrossixConstants.OUTPUT_FILE_PATH + "/output_file.csv");
            for (int i = 0; i < listOfFiles.length; i++) {
                BufferedReader reader = new BufferedReader(new FileReader(listOfFiles[i]));
                readers.add(reader);
                String line = reader.readLine();
                if (line != null) {
                    map.put(prepareInputData(line.split(",")), readers.get(i));
                }
            }

            List<InputData> sortedKeyInputData = new LinkedList<>(map.keySet());
            while ( map.size() > 0 ) {
                Collections.sort(sortedKeyInputData, Comparator.comparing(InputData::getVisitorId));
                InputData inputData = sortedKeyInputData.remove(0);
                writer.write(inputData.toString() + "\n");
                BufferedReader reader = map.remove(inputData);
                String nextLine = reader.readLine();
                if ( nextLine != null ){
                    InputData sw = prepareInputData(nextLine.split(","));
                    map.put(sw,  reader);
                    sortedKeyInputData.add(sw);
                }
            }
        }catch (Exception e){
            logger.error("mergeChunksAndWriteToOutput failed", e);
        } finally {
            for (BufferedReader reader: readers){
                try{
                    reader.close();
                } catch(Exception e){
                    logger.error("mergeChunksAndWriteToOutput failed", e);
                }
            }
            try{
                writer.close();
            } catch(Exception e){
                logger.error("mergeChunksAndWriteToOutput failed", e);
            }
        }
        logger.info("mergeChunksAndWriteToOutput finished successfully!");
    }
}