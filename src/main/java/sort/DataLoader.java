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
    public void loadCSVAndSplitData(String inputPath) {
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
                    System.out.println("writing chunk #" + chunk_counter + " total " + line_counter + " lines.");
                }
            }
            //UPDATED: flush rest of lines
            if(!tempList.isEmpty()){
                sortInputDataList(tempList);
                chunk_counter++;
                writeInputDataChunk(tempList, chunk_counter);
                tempList.clear();
                System.out.println("writing chunk #" + chunk_counter + " total " + line_counter + " lines.");
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
     * read every 2 files of chunks, merge them on temp file, till all files are merged on the sorted manner
     * into the final output folder (output_data).
     */
    public void readChunksAndMergeSortWrite(String chunkPath) {
        File dir = new File(chunkPath);
        File[] listOfFiles = dir.listFiles();
        try {
            int numOfFiles = listOfFiles.length;
            int countOfFiles = 0;
            //there is only one file, write it to the output
            if(dir.list().length==1){
                PrintWriter writer = new PrintWriter(CrossixConstants.OUTPUT_FILE_PATH + "/output_file.csv");
                BufferedReader reader = new BufferedReader(new FileReader(listOfFiles[0]));
                String line = reader.readLine();
                while(line!=null) {
                    InputData inputData = prepareInputData(line.split(","));
                    writer.write(inputData.toString() + "\n");
                    line = reader.readLine();
                }
                listOfFiles[0].delete();
                reader.close();
                writer.close();
            } else {
                //merge all pairs of files
                int numOfFileToMerge = numOfFiles%2==0 ? numOfFiles:numOfFiles-1;
                boolean mergeFlag = true;
                int totalCount = 0;
                while (mergeFlag) {
                    for (int i = 0; i < numOfFileToMerge; i += 2) {
                        totalCount++;
                        mergeTwoFiles(listOfFiles[i], listOfFiles[i + 1], CrossixConstants.CHUNKS_PATH + "/temp_merge_" + totalCount + ".csv");
                        countOfFiles+=2;
                    }
                    mergeFlag = dir.listFiles().length > 2 ? true: false;
                    listOfFiles = dir.listFiles();
                    numOfFileToMerge = listOfFiles.length%2==0 ? listOfFiles.length:listOfFiles.length-1;
                }
                //merge last odd file with one of the merged files
                if (countOfFiles < numOfFiles) {
                    mergeTwoFiles(listOfFiles[countOfFiles], new File(CrossixConstants.CHUNKS_PATH + "/temp_merge_" + totalCount + ".csv"), CrossixConstants.CHUNKS_PATH + "/temp_merge_odd.csv");
                }
                //merge last 2 merged files into final output file with all sorted data
                listOfFiles = dir.listFiles();
                mergeTwoFiles(listOfFiles[0], listOfFiles[1], CrossixConstants.OUTPUT_FILE_PATH + "/output_file.csv");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void mergeTwoFiles(File file1, File file2, String outputFileName) {
        try {
            PrintWriter writer = new PrintWriter(outputFileName);
            BufferedReader reader1 = new BufferedReader(new FileReader(file1));
            BufferedReader reader2 = new BufferedReader(new FileReader(file2));
            String line1 = reader1.readLine();
            String line2 = reader2.readLine();
            InputData inputData1 = prepareInputData(line1.split(","));
            InputData inputData2 = prepareInputData(line2.split(","));
            while (line1 != null || line2 != null) {
                if (line1 != null && inputData1.getVisitorId().compareTo(inputData2.visitorId) < 1) {
                    writer.write(inputData1.toString() + "\n");
                    line1 = reader1.readLine();
                    if (line1 != null) {
                        inputData1 = prepareInputData(line1.split(","));
                    }
                } else if (line2 != null && inputData1.getVisitorId().compareTo(inputData2.visitorId) >= 1) {
                    writer.write(inputData2.toString() + "\n");
                    line2 = reader2.readLine();
                    if (line2 != null)
                        inputData2 = prepareInputData(line2.split(","));
                } else if (line1 == null && line2 != null) {
                    writer.write(inputData2.toString() + "\n");
                    line2 = reader2.readLine();
                    if (line2 != null)
                        inputData2 = prepareInputData(line2.split(","));
                } else if (line2 == null && line1 != null) {
                    writer.write(inputData1.toString() + "\n");
                    line1 = reader1.readLine();
                    if (line1 != null)
                        inputData1 = prepareInputData(line1.split(","));
                }
            }
            writer.close();
            reader1.close();
            reader2.close();
            file1.delete();
            file2.delete();
        } catch (IOException e){
            logger.error("Error on merge files: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}