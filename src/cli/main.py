import click
from src.cli.commands import download, generate, transformation_preprocess, transformation_bars

@click.group()
def main():
    pass

main.add_command(generate.generate)
main.add_command(download.download)
main.add_command(transformation_preprocess.transformation_preprocess)
main.add_command(transformation_bars.transformation_bars)

if __name__ == "__main__":
    main()
