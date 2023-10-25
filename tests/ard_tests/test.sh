# Basic tests for the common util module
# for the dynamically generated config files

#!/bin/bash
source ../dynamic_config_file.sh

generate_dynamic_config_file "gadi"

echo " no host given...."

generate_dynamic_config_file

echo "Cleaning up config file"
clean_up_dynamic_config_file
