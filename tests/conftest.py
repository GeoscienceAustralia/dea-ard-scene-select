# conftest.py

import pytest

@pytest.fixture
def set_up_dirs_and_db():
    # Your setup code for creating a temporary database and other configurations
    # This function should return any values or resources needed for your tests
    setup_script = Path(__file__).parent.joinpath("db_index.sh")
    cmd = [setup_script]
    try:
        cmd_stdout = check_output(cmd, stderr=STDOUT, shell=True).decode()
    except Exception as e:
        print(e.output.decode())  # print out the stdout messages up to the exception
        print(e)  # To print out the exception message
    print("====================")
    print(cmd_stdout)
    print("====================")

    # So the scene select call uses the correct DB
    if "HOSTNAME" in os.environ and "gadi" in os.environ["HOSTNAME"]:
        # Nobody call their system Brigadiers, ok.
        end_tag = "_dev"
    else:
        end_tag = "_local"
    os.environ["DATACUBE_ENVIRONMENT"] = f"{os.getenv('USER')}{end_tag}"
    os.environ["DATACUBE_CONFIG_PATH"] = str(
        Path(__file__).parent.joinpath("datacube.conf")
    )

    # Return any relevant values/resources needed for your tests
    return some_value, some_other_value
