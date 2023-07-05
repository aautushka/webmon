def validate(source, sink) -> None:
    while message := source.get():
        sink.put(message)
