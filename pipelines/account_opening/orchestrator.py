import subprocess
import time

conv_main = "D:/pipelines/account_opening/conventional_main.py"
tab_main = "D:/pipelines/account_opening/TAB_main.py"
tab_restriction = "D:/pipelines/account_opening/TAB_restriction_main.py"


def run_etl():
    try:
        subprocess.run(["python", conv_main])
    except Exception as e:
        print(f"conventional_main could not be executed: {str(e)}")

    time.sleep(5)
    try:
        subprocess.run(["python", tab_main])
    except Exception as e:
        print(f"TAB_main could not be executed: {str(e)}")

    time.sleep(5)
    try:
        subprocess.run(["python", tab_restriction])
    except Exception as e:
        print(f"TAB_restriction_main could not be executed: {str(e)}")


if __name__ == "__main__":
    run_etl()
