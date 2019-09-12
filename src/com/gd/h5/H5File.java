package com.gd.h5;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.HDFNativeData;
import hdf.hdf5lib.exceptions.HDF5LibraryException;


import java.util.HashMap;
import java.util.Map;

public class H5File {
    long fileHandle = -1;
    Map<String, H5Dataset> openedDatasets = new HashMap<>();

    public H5File(String file) {
        fileHandle = H5.H5Fopen(file, HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
    }


    public long[] getDatasetDims(long dataset) {
        long dataspace = H5.H5Dget_space(dataset);
        long rank = H5.H5Sget_simple_extent_ndims(dataspace);
        long[] dims = new long[(int) rank];
        H5.H5Sget_simple_extent_dims(dataspace, dims, null);
        return dims;
    }


    private int getDataSetRank(long dataset) {
        long dataspace = H5.H5Dget_space(dataset);
        return H5.H5Sget_simple_extent_ndims(dataspace);
    }


    public static String getDataTypeName(long dataType) {
        return H5.H5Tget_class_name(dataType);
    }

    public H5Dataset createDataset(String name, long dataType, long[] dimensions, long[] maxDimension) {
        if (openedDatasets.containsKey(name)) {
            return openedDatasets.get(name);
        }
        long fileSpace = H5.H5Screate_simple(dimensions.length, dimensions, maxDimension);
        long handle = H5.H5Dcreate(fileHandle, name,
                dataType, fileSpace,
                HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT);
        H5Dataset dataset = new H5Dataset(handle, dimensions.length, dataType);
        openedDatasets.put(name, dataset);
        return dataset;
    }

    public H5Dataset openDataSet(String name) {
        if (openedDatasets.containsKey(name)) {
            return openedDatasets.get(name);
        }

        try {
            long dataset = H5.H5Dopen(fileHandle, name, HDF5Constants.H5P_DEFAULT);
            H5Dataset ds = new H5Dataset(dataset, getDataSetRank(dataset), H5.H5Dget_type(dataset));
            openedDatasets.put(name, ds);
            return ds;
        } catch (HDF5LibraryException e) {

        }
        return null;
    }

    public Object readData(String name, int index, int count) {
        //
        H5Dataset dataset = openedDatasets.get(name);
        long dataspace = H5.H5Dget_space(dataset.datasetHandle);

        long[] offset = getDatasetDims(dataset.getRank());
        long[] dimCount = offset.clone();
        dimCount[0] = count;


        //select memory space
        long memspace = H5.H5Screate_simple(dataset.getRank(), dimCount, null);
        H5.H5Sselect_hyperslab(memspace, HDF5Constants.H5S_SELECT_SET, offset, null, dimCount, null);

        int valueCount = 1;
        for (int i = 0; i < dimCount.length; i++) {
            valueCount *= dimCount[i];
        }
        byte[] values = new byte[(int)dataset.getTypeSize() * valueCount];
        //select data space
        offset[0] = index;
        H5.H5Sselect_hyperslab(dataspace, HDF5Constants.H5S_SELECT_SET, offset, null, dimCount, null);


        H5.H5Dread(dataset.datasetHandle, dataset.getRank(), memspace, dataset.datasetHandle, HDF5Constants.H5P_DEFAULT, values);

        return values;

    }

    public static class H5Dataset {
        long datasetHandle;
        int rank;
        long[] dimensions;
        long type;
        long typeSize;

        public long getType() {
            return type;
        }

        public long getTypeSize() {
            return typeSize;
        }

        public H5Dataset(long datasetHandle, int rank, long type) {
            this.datasetHandle = datasetHandle;
            this.rank = rank;
            this.type = H5.H5Tget_class(type);
            this.typeSize = H5.H5Tget_size(type);
        }

        public int getRank() {
            return rank;
        }

        public long[] getDimensions() {
            return dimensions;
        }

        public void setDimensions(long[] dimensions) {
            this.dimensions = dimensions;
        }
    }
}
