from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[tuple[int, str]] = []
        self._counter = 0
        self._processed = 0

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
            self._processed += 1

        else:
            for value in data:
                self._data.append((self._counter, str(value)))
                self._counter += 1
                self._processed += 1


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
            self._processed += 1

        else:
            for value in data:
                self._data.append((self._counter, value))
                self._counter += 1
                self._processed += 1


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
            self._processed += 1

        else:
            for value in data:
                log = (
                    f"{value['log_level']}: "
                    f"{value['log_message']}"
                )
                self._data.append((self._counter, log))
                self._counter += 1
                self._processed += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def print_processors_stats(self) -> None:
        if not self._processors:
            print("No processor found, no data")

        for proc in self._processors:
            name = type(proc).__name__
            print(
                f"{name}: total {proc._processed} items processed, "
                f"remaining {len(proc._data)} on processor"
            )

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    processed = True
                    break

            if not processed:
                print(
                    "DataStream error - "
                    f"Can't process element in stream: {element}"
                )

    def output_pipeline(self, nb: int, plugin: "ExportPlugin") -> None:

        for proc in self._processors:

            exported_data: list[tuple[int, str]] = []

            for _ in range(nb):

                if not proc._data:
                    break

                exported_data.append(proc.output())

            if exported_data:
                plugin.process_output(exported_data)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:

        values = [value for _, value in data]
        print("CSV Output:")
        print(",".join(values))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:

        print("JSON Output:")

        items = []

        for key, value in data:
            items.append(f'"item_{key}": "{value}"')

        print("{" + ",".join(items) + "}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")
    stream = DataStream()

    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print()

    print("Registering Processors\n")
    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    stream.register_processor(num)
    stream.register_processor(text)
    stream.register_processor(log)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet acces! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print(f"Send first batch of data on stream: {batch}\n")

    print("== DataStream statistics ==")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print()

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv = CSVExportPlugin()
    stream.output_pipeline(3, csv)
    print()

    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print()
    batch2 = [
        21,
        [
            "I Love AI",
            "LLMs are wonderful",
            "Stay healthy"
        ],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash"
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print(f"Send another batch o data: {batch2}\n")

    print("== DataStream statistics ==")
    stream.process_stream(batch2)
    stream.print_processors_stats()
    print()

    print("Send 5 processed data form each processor to a JSON plugin:")
    json = JSONExportPlugin()
    stream.output_pipeline(5, json)
    print()

    print("==DataStream statistics ==")
    stream.print_processors_stats()
