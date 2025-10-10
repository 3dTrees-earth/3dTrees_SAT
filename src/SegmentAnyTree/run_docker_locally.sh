#!/bin/bash

CONTAINER_NAME="test_e2e_instance"
IMAGE_NAME="nibio/e2e-instance"

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <source_path_1> <source_path_2>"
    exit 1
fi

SOURCE_PATH_1=$1
SOURCE_PATH_2=$2

# Check if the container exists
if [ $(docker container ls -a -q -f name=$CONTAINER_NAME) ]; then
    echo "Removing existing container $CONTAINER_NAME"
    docker container rm $CONTAINER_NAME
else
    echo "Container $CONTAINER_NAME does not exist."
fi

# Check if the image exists
# if [ $(docker image ls -q -f reference=$IMAGE_NAME) ]; then
#     echo "Removing existing image $IMAGE_NAME"
#     docker image rm $IMAGE_NAME
# else
#     echo "Image $IMAGE_NAME does not exist."
# fi

# ./build.sh
docker build -t $IMAGE_NAME ./SegmentAnyTree

echo "Running the container"
# docker run -it --gpus all --name $CONTAINER_NAME $IMAGE_NAME > e2e-instance.log 2>&1

#docker run -it --gpus all \
#    --name $CONTAINER_NAME \
#    --memory=100g \
#    --memory-swap=100g \  # Prevent the container from swapping memory
#    --oom-kill-disable \  # Allow the container to be killed if it exceeds the memory limit
#    --mount type=bind,source=/home/teja/segmentanytree_in/,target=/home/nibio/mutable-outside-world/bucket_in_folder \
#    --mount type=bind,source=/home/teja/segmentanytree_out/,target=/home/nibio/mutable-outside-world/bucket_out_folder \
#    $IMAGE_NAME 

#docker run -it --gpus all  \
docker run -it --gpus all \
    --name $CONTAINER_NAME \
    --memory=150g \
    --memory-swap=150g \
    --mount type=bind,source=$SOURCE_PATH_1,target=/home/nibio/mutable-outside-world/bucket_in_folder \
    --mount type=bind,source=$SOURCE_PATH_2,target=/home/nibio/mutable-outside-world/bucket_out_folder \
    $IMAGE_NAME
    
    #--memory=100g \
    #--memory-swap=100g \
    #--oom-kill-disable \

