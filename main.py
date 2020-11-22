from utils.FileLoader import FileLoader
from utils.driver import run


if __name__ == "__main__":

    loader = FileLoader("test_files/test23.txt")

    case_id = 1
    while loader.has_next():
        print(f"Test {case_id} Result")
        c = loader.next_case()
        run(c)
        case_id += 1
