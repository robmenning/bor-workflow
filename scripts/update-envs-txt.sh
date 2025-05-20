#!/bin/bash

# Clear the output file first
> envs.txt

# Loop through all .env* files in the current directory
for env_file in ./.env*; do
    if [ -f "$env_file" ]; then
        # Get just the filename without the path
        filename=$(basename "$env_file")
        
        # Add section header
        echo "#################### $filename ####################" >> envs.txt
        echo "" >> envs.txt
        
        # Append file contents
        cat "$env_file" >> envs.txt
        
        # Add extra newline for readability
        echo "" >> envs.txt
        echo "" >> envs.txt
    fi
done

echo "Updated envs.txt with contents of all .env* files"
