import typer

app = typer.Typer()


@app.command()
def main(port: int = 8080):
    from . import _loop

    _loop.start(port)
