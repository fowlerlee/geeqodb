#ifndef GEEQODB_CUDA_WRAPPER_H
#define GEEQODB_CUDA_WRAPPER_H

#include <stddef.h>
#include <stdint.h>

// OpenGL type definitions
#ifndef GEEQODB_NO_GL
#ifdef __APPLE__
#include <OpenGL/gl.h>
#else
#include <GL/gl.h>
#endif
#else
// Define OpenGL types if not including GL headers
typedef unsigned int GLuint;
#endif

// CUDA driver API types
// Define CUDA types without requiring actual CUDA headers
// This allows the code to compile without CUDA installed
#define GEEQODB_NO_CUDA
#ifndef GEEQODB_NO_CUDA
#include <cuda.h>
#else
// Define CUDA types if not including CUDA headers
typedef struct CUgraphicsResource_st *CUgraphicsResource;
typedef struct CUstream_st *CUstream;
typedef void *CUdeviceptr;
#endif

#ifdef __cplusplus
extern "C"
{
#endif

    // Error codes
    typedef enum
    {
        CUDA_SUCCESS = 0,
        CUDA_ERROR_INIT_FAILED = 1,
        CUDA_ERROR_NO_DEVICE = 2,
        CUDA_ERROR_MEMORY_ALLOCATION = 3,
        CUDA_ERROR_LAUNCH_FAILED = 4,
        CUDA_ERROR_INVALID_VALUE = 5,
        CUDA_ERROR_NOT_SUPPORTED = 6,
        CUDA_ERROR_UNKNOWN = 999
    } CudaError;

    // Device information
    typedef struct
    {
        int device_id;
        char name[256];
        size_t total_memory;
        int compute_capability_major;
        int compute_capability_minor;
        int multi_processor_count;
        int max_threads_per_block;
    } CudaDeviceInfo;

    // Buffer handle
    typedef struct
    {
        void *device_ptr;
        size_t size;
        void *count_ptr; // For operations that need to track result counts
    } CudaBuffer;

    // Comparison operators for filter operations
    typedef enum
    {
        CUDA_CMP_EQ = 0,
        CUDA_CMP_NE = 1,
        CUDA_CMP_LT = 2,
        CUDA_CMP_LE = 3,
        CUDA_CMP_GT = 4,
        CUDA_CMP_GE = 5,
        CUDA_CMP_BETWEEN = 6
    } CudaComparisonOp;

    // Join types
    typedef enum
    {
        CUDA_JOIN_INNER = 0,
        CUDA_JOIN_LEFT = 1,
        CUDA_JOIN_RIGHT = 2,
        CUDA_JOIN_FULL = 3
    } CudaJoinType;

    // Aggregation operations
    typedef enum
    {
        CUDA_AGG_SUM = 0,
        CUDA_AGG_COUNT = 1,
        CUDA_AGG_MIN = 2,
        CUDA_AGG_MAX = 3,
        CUDA_AGG_AVG = 4
    } CudaAggregateOp;

    // Data types
    typedef enum
    {
        CUDA_TYPE_INT32 = 0,
        CUDA_TYPE_INT64 = 1,
        CUDA_TYPE_FLOAT = 2,
        CUDA_TYPE_DOUBLE = 3,
        CUDA_TYPE_STRING = 4
    } CudaDataType;

    // Initialize CUDA and get device count
    CudaError cuda_init(int *device_count);

    // Get device information
    CudaError cuda_get_device_info(int device_id, CudaDeviceInfo *info);

    // Allocate memory on the GPU
    CudaError cuda_allocate(int device_id, size_t size, CudaBuffer *buffer);

    // Free memory on the GPU
    CudaError cuda_free(CudaBuffer buffer);

    // Copy data from host to device
    CudaError cuda_copy_to_device(void *host_ptr, CudaBuffer buffer, size_t size);

    // Copy data from device to host
    CudaError cuda_copy_to_host(CudaBuffer buffer, void *host_ptr, size_t size);

    // Execute filter operation on the GPU (simulation)
    CudaError cuda_execute_filter(
        CudaBuffer input,
        CudaBuffer output,
        CudaComparisonOp op,
        CudaDataType data_type,
        void *value,
        void *value2 // For BETWEEN operations
    );

    // Execute filter operation on the GPU (real implementation)
    CudaError cuda_execute_filter_real(
        CudaBuffer input,
        CudaBuffer output,
        CudaComparisonOp op,
        CudaDataType data_type,
        void *value,
        void *value2, // For BETWEEN operations
        size_t num_rows);

    // Execute hash join operation on the GPU
    CudaError cuda_execute_hash_join(
        CudaBuffer left_keys,
        CudaBuffer left_values,
        CudaBuffer right_keys,
        CudaBuffer right_values,
        CudaBuffer output_keys,
        CudaBuffer output_left_values,
        CudaBuffer output_right_values,
        size_t left_size,
        size_t right_size);

    // Execute window function on the GPU
    CudaError cuda_execute_window_function(
        CudaBuffer input,
        CudaBuffer output,
        CudaDataType data_type,
        size_t num_rows);

    // Execute join operation on the GPU
    CudaError cuda_execute_join(
        CudaBuffer left,
        CudaBuffer right,
        CudaBuffer output,
        CudaJoinType join_type,
        int left_join_col,
        int right_join_col,
        CudaDataType data_type);

    // Execute aggregation operation on the GPU
    CudaError cuda_execute_aggregate(
        CudaBuffer input,
        CudaBuffer output,
        CudaAggregateOp op,
        CudaDataType data_type,
        int column_index);

    // Execute sort operation on the GPU
    CudaError cuda_execute_sort(
        CudaBuffer input,
        CudaBuffer output,
        CudaDataType data_type,
        int column_index,
        int ascending);

    // Execute group by operation on the GPU
    CudaError cuda_execute_group_by(
        CudaBuffer input,
        CudaBuffer output,
        CudaDataType group_type,
        int group_column,
        CudaDataType agg_type,
        int agg_column,
        CudaAggregateOp agg_op);

    // Get error string
    const char *cuda_get_error_string(CudaError error);

    // OpenGL interoperability functions
    CudaError cuda_gl_register_buffer(GLuint buffer, CUgraphicsResource *resource);
    CudaError cuda_graphics_map_resources(CUgraphicsResource resource, CUstream stream);
    CudaError cuda_graphics_get_mapped_pointer(CUdeviceptr *devPtr, size_t *size,
                                               CUgraphicsResource resource);
    CudaError cuda_graphics_unmap_resources(CUgraphicsResource resource, CUstream stream);
    CudaError cuda_graphics_unregister_resource(CUgraphicsResource resource);

#ifdef __cplusplus
}
#endif

#endif // GEEQODB_CUDA_WRAPPER_H
