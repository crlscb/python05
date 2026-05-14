from abc import ABC, abstractmethod
from typing import Any, cast


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._data.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)

        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, (int, float)):
            self._data.append((self._counter, str(data)))
            self._counter += 1

        else:
            for value in data:
                self._data.append((self._counter, str(value)))
                self._counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, str):
            self._data.append((self._counter, data))
            self._counter += 1

        else:
            for value in data:
                self._data.append((self._counter, value))
                self._counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (
                all(isinstance(x, str) for x in data.keys())
                and
                all(isinstance(x, str) for x in data.values())
            )

        if isinstance(data, list):
            return all(self.validate(x) for x in data)

        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, dict):
            log = (
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )
            self._data.append((self._counter, log))
            self._counter += 1

        else:
            for value in data:
                log = (
                    f"{value['log_level']}: "
                    f"{value['log_message']}"
                )
                self._data.append((self._counter, log))
                self._counter += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    num = NumericProcessor()
    print(f"Trying to validate input '42' {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest(cast(Any, 'foo'))
    except ValueError as error:
        print(f"Got exception: {error}")

    print("Processing data: [1, 2, 3, 4, 5]")
    num.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        id_value, value = num.output()
        print(f"Numeric value {id_value}: {value}")
    print()

    print("Testing Test Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text.ingest(["Hello", "Nexus", "World"])
    print("Extracting 1 value...")
    for _ in range(1):
        id_value, value = text.output()
        print(f"Text value {id_value}: {value}")
    print()

    print("Testing Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")
    print(
        "Processing data: [{'log_level': 'NOTICE', "
        "'log_message': 'Connection to server'}, "
        "{'log_level': 'ERROR', "
        "'log_message': 'Unauthorized access!!'}]"
        )
    log.ingest(
        [
            {
                'log_level': 'NOTICE',
                'log_message': 'Connection to server'
            },
            {
                'log_level': 'ERROR',
                'log_message': 'Unauthorized access!!'
            }
        ]
    )
    print("Extracting 2 values...")
    for _ in range(2):
        if_value, value = log.output()
        print(f"Log entry {id_value}: {value}")
