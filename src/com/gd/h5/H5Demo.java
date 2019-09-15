/*****************************************************************************
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of the HDF Java Products distribution.                  *
 * The full copyright notice, including terms governing use, modification,   *
 * and redistribution, is contained in the file, COPYING.                    *
 * COPYING can be found at the root of the source code distribution tree.    *
 * If you do not have access to this file, you may request a copy from       *
 * help@hdfgroup.org.                                                        *
 ****************************************************************************/

/************************************************************
 This example shows how to read and write data to a
 dataset by hyberslabs.  The program first writes integers
 in a hyperslab selection to a dataset with dataspace
 dimensions of DIM_XxDIM_Y, then closes the file.  Next, it
 reopens the file, reads back the data, and outputs it to
 the screen.  Finally it reads the data again using a
 different hyperslab selection, and outputs the result to
 the screen.
 ************************************************************/
package com.gd.h5;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5FileInterfaceException;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

/**
 *
 * @author xue
 */

 class MsgFormater extends Formatter{
    @Override
    public String format(LogRecord record) {
        return String.format(record.getMessage());
    }

    protected MsgFormater() {
        super();
    }
}

public class H5Demo {

    private static String FILENAME = "H5Ex_D_Hyperslab.h5";
    private static String DATASETNAME = "DS2";
    private static final int DIM_X = 5000;
    private static final int DIM_Y = 30;
    private static final int RANK = 2;

    private static void writeHyperslab() {
        long file_id = -1;
        long filespace_id = -1;
        long memspace_id = -1;
        long dataset_id = -1;
        long[] dims = { DIM_X, DIM_Y };
        long[] dims_in = { 1, DIM_Y };

        int[][] dset_data = new int[DIM_X][DIM_Y];

        for (int j = 0; j < DIM_X; j++){
            for (int i = 0; i < DIM_Y; i++)
            {
                dset_data[j][i] = i + j;
            }
        }

        // Create a new file using default properties.
        try {
            file_id = H5.H5Fopen(FILENAME, HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Create dataspace. Setting maximum size to NULL sets the maximum
        // size to be the current size.
        try {
            filespace_id = H5.H5Screate_simple(RANK, dims, null);
            memspace_id = H5.H5Screate_simple(RANK, dims_in, null);

        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Create the dataset. We will use all default properties for this example.
        try {
            if ((file_id >= 0) && (filespace_id >= 0))
                dataset_id = H5.H5Dcreate(file_id, DATASETNAME,
                        HDF5Constants.H5T_STD_I32LE, filespace_id,
                        HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Define and select the first part of the hyperslab selection.
        long[] start = { 0, 0 };
        long[] count = { 1, DIM_Y };


        // Define and select the second part of the hyperslab selection,
        // which is subtracted from the first selection by the use of
        // H5S_SELECT_NOTB
        try {
            if ((filespace_id >= 0)) {
                // Write the data to the dataset.

                    long before = System.currentTimeMillis();

                for (int i = 0; i < DIM_X ; i++) {
                        start[0] = i;

                        H5.H5Sselect_hyperslab(filespace_id, HDF5Constants.H5S_SELECT_SET,
                                start, null, count, null);
                        H5.H5Dwrite(dataset_id, HDF5Constants.H5T_NATIVE_INT,
                                memspace_id, filespace_id, HDF5Constants.H5P_DEFAULT,
                                dset_data[i]);
                    }
                System.out.println(String.format("elapse time:%d ms", System.currentTimeMillis() - before));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // End access to the dataset and release resources used by it.
        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if (filespace_id >= 0)
                H5.H5Sclose(memspace_id);
                H5.H5Sclose(filespace_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        // Close the file.
        try {
            if (file_id >= 0)
                H5.H5Fclose(file_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readHyperslab() {
        long file_id = -1;
        long filespace_id = -1;
        long dataset_id = -1;
        int dcpl_id = -1;
        int[][] dset_data = new int[DIM_X][DIM_Y];

        // Open an existing file.
        try {
            file_id = H5.H5Fopen(FILENAME, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Open an existing dataset.
        try {
            if (file_id >= 0)
                dataset_id = H5.H5Dopen(file_id, DATASETNAME, HDF5Constants.H5P_DEFAULT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Read the data using the default properties.
        try {
            if (dataset_id >= 0)
                H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_INT,
                        HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
                        HDF5Constants.H5P_DEFAULT, dset_data);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Output the data to the screen.
        System.out.println("Data as written to disk by hyberslabs:");
        for (int indx = 0; indx < DIM_X; indx++) {
            System.out.print(" [ ");
            for (int jndx = 0; jndx < DIM_Y; jndx++)
                System.out.print(dset_data[indx][jndx] + " ");
            System.out.println("]");
        }
        System.out.println();

        // Initialize the read array.
        for (int indx = 0; indx < DIM_X; indx++)
            for (int jndx = 0; jndx < DIM_Y; jndx++)
                dset_data[indx][jndx] = 0;

        // Define and select the hyperslab to use for reading.
        try {
            if (dataset_id >= 0) {
                filespace_id = H5.H5Dget_space(dataset_id);

                long[] start = { 0, 1 };
                long[] stride = { 4, 4 };
                long[] count = { 2, 2 };
                long[] block = { 2, 3 };


                if (filespace_id >= 0) {
                    H5.H5Sselect_hyperslab(filespace_id, HDF5Constants.H5S_SELECT_SET,
                            start, stride, count, block);

                    // Read the data using the previously defined hyperslab.
                    if ((dataset_id >= 0) && (filespace_id >= 0))
                        H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_INT,
                                HDF5Constants.H5S_ALL, filespace_id, HDF5Constants.H5P_DEFAULT,
                                dset_data);
                }

            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Output the data to the screen.
        System.out.println("Data as read from disk by hyberslab:");
        for (int indx = 0; indx < DIM_X; indx++) {
            System.out.print(" [ ");
            for (int jndx = 0; jndx < DIM_Y; jndx++)
                System.out.print(dset_data[indx][jndx] + " ");
            System.out.println("]");
        }
        System.out.println();

        // End access to the dataset and release resources used by it.
        try {
            if (dcpl_id >= 0)
                H5.H5Pclose(dcpl_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if (dataset_id >= 0)
                H5.H5Dclose(dataset_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            if (filespace_id >= 0)
                H5.H5Sclose(filespace_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Close the file.
        try {
            if (file_id >= 0)
                H5.H5Fclose(file_id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        H5File file = new H5File("E:\\04.code\\h5\\h5_test\\big.cgd");
        H5File.H5Dataset ds = file.openDataSet("AMPN");
        if (ds == null) {
            System.out.println("open data set failed");
            return;
        }

        long rank = ds.getRank();
        String type = file.getDataTypeName(ds.getTypeClass());
        System.out.println(String.format("%s rank is %d and data type is %s", "DS2", rank, type));

        Random rd = new Random();

        AtomicInteger ai = new AtomicInteger();
        for (int i = 0; i < 1; i++) {
            final int offset = rd.nextInt(1000);
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    Logger logger = Logger.getLogger(String.valueOf(offset));
                    try {

//                        FileHandler handler = new FileHandler(String.format("logs%d.txt", ai.getAndAdd(1)));
//                        MsgFormater sf = new MsgFormater();
//                        logger.setLevel(Level.INFO);
//                        handler.setFormatter(sf);
//                        logger.addHandler(handler);
                        long before = System.currentTimeMillis();
                        for (int j =offset; j < 5000 + offset; j++) {
                            final byte[] bytes = ds.readData(j, 1);
                           // float[] values = H5File.byte2float(bytes);
//                            logger.info(String.valueOf(j) + "\t");
//                            for (int k = 0; k < values.length; k++) {
//                                logger.info(values[k] + " ");
//                            }
//                            logger.info("\n");
                        }

                        System.out.println(System.currentTimeMillis() - before);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            });
            t.start();
        }

    }

}
