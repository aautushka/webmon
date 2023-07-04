def report(source, sink):
    while True:
        request = source.get()
        print(request)
