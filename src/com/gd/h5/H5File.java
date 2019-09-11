package com.gd.h5;

import hdf.hdf5lib.H5;
import hdf.hdf5lib.HDF5Constants;
import hdf.hdf5lib.exceptions.HDF5LibraryException;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class H5File {
    long fileHandle = -1;
    Map<String, Long> openedDatasets = new HashMap<>();

    public H5File(String file) {
        fileHandle = H5.H5Fopen(file, HDF5Constants.H5F_ACC_RDWR, HDF5Constants.H5P_DEFAULT);
    }


    public long[] getDatasetDims(long dataset) {
        long dataspace = H5.H5Dget_space(ds);
        long rank = H5.H5Sget_simple_extent_ndims(dataspace);
        long [] dims = new long[(int)rank];
        H5.H5Sget_simple_extent_dims(dataspace, dims, null);
        return dims;
    }


    public long getDatasetRank(long dataset) {

        long dataspace = H5.H5Dget_space(ds);
        return H5.H5Sget_simple_extent_ndims(dataspace);
    }


    public long getDataSetType(long dataset) {
        return H5.H5Tget_class(H5.H5Dget_type(dataset));
    }

     public String getDataTypeName(long dataset){
        long dataType = getDataSetType(dataset);
        return H5.H5Tget_class_name(dataType);
    }

    public long createDataset(String name, long dataType, long[] dimensions, long[] maxDimension) {
        if (openedDatasets.containsKey(name)){
            return openedDatasets.get(name);
        }
        long fileSpace = H5.H5Screate_simple(dimensions.length, dimensions, maxDimension );
        long ds  = H5.H5Dcreate(fileHandle, name,
                dataType, fileSpace,
                HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT, HDF5Constants.H5P_DEFAULT) ;

        openedDatasets.put(name, ds);
        return ds;
    }

    public long openDataset(String name) {
        if (openedDatasets.containsKey(name)){
            return openedDatasets.get(name);
        }
        long ds = -1;
        try {
            ds = H5.H5Dopen(fileHandle, name, HDF5Constants.H5P_DEFAULT);
            openedDatasets.put(name, ds);
            return ds;
        } catch (HDF5LibraryException e) {

        }
        return -1;
    }

    public int readData(String name, int index, int count, Object vals) {
        //
        long ds =  openedDatasets.get(name);
        long dataspace = H5.H5Dget_space(ds);
        long[] offset = getDatasetDims(ds);
        long[] dimCount = offset.clone();
        dimCount[0] = count;

        H5.H5Sselect_hyperslab(dataspace, HDF5Constants.H5S_SELECT_SET, offset,null, dimCount,null);


        H5.H5Dread(ds, getDataSetType(ds), ,ds, HDF5Constants.H5P_DEFAULT, vals);

    }
}
