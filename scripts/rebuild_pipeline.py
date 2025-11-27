from scripts.init_db import main as init_db_main
from scripts.load_to_raw import main as load_raw_main
from scripts.build_models import main as build_models_main


def main():
    init_db_main()
    load_raw_main()
    build_models_main()
    print("Full pipeline run complete.")


if __name__ == "__main__":
    main()