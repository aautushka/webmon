def report(source, sink) -> None:
    while request := source.get():
        print(request)
