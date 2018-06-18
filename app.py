import click

@click.command()
@click.option('--count', default=1, help='Number of greetings.')
@click.option('--name', prompt='Your name',
              help='The person to greet.')
def main(count, name):
    i = input("Choose an option (e-exit, f-add files, q-query)");
    while i != "e":
        if i == "f":
            open_files()
        elif i == "q":
            query()
        else: #nothing matched
            print "Nothing matched\n"
    #for x in range(count):
    #    click.echo('Hello %s!' % name)

def open_files():
    i = input("Type in a comma seperated list of files: ")
    return

def query():
    print """The query must be as follows:
             SELECT attr1, attr2, .... attrn
             FROM R1, R2, ... Rn
             WERE C1 AND ... AND Ck /n"""

    return

if __name__ == '__main__':
    main()
