# Generate a config file on the fly.
# The file name will resemble the "${USER}_dev.conf" pattern.
# If a host name is given (at the moment, it's typically 'gadi'),
# it reflects an NCI run.
generate_dynamic_config_file() {
    local hostname="local"
    local db_hostname="localhost"
    local config_file="${USER}_dev.conf"

    if [ $# -eq 1 ]; then
        hostname=$1
        db_hostname="deadev.nci.org.au"

        config_content="[datacube]
db_hostname: $db_hostname
db_port: 6432
db_database: ${USER}_dev"
        config_file="${USER}_dev.conf"
    else
        # Default configuration when hostname is not provided.
        # This is assuming a local (non-gadi) run
        echo "Non-NCI run"

        config_content="[datacube]
db_hostname: $db_hostname
db_database: ${USER}_local"
    fi

    echo "$config_content" > "$config_file"
    echo "Config file, '${config_file}' generated"
}

# Perform clean up of the dynamic config file that was generated on the fly.
# The file name will resemble the "${USER}_dev.conf" pattern.
clean_up_dynamic_config_file() {
    config_file="${USER}_dev.conf"

    echo "Cleaning up dynamic config file, '$config_file'..."
    rm -f "./$config_file"

    if [ ! -e "./$config_file" ]; then
        echo "Cleaned up $config_file"
    else
        echo "Error: $config_file cannot be removed"
    fi
}
