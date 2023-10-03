import sys
import re

if len(sys.argv) < 3:
    print('USAGE: filterDumps.py INPUT_FILE LIST_OF_PAGE_IDS')
    sys.exit(1)

inputFile = sys.argv[1]
pagesToFind = sys.argv[2].split(',')
lastFewLines = []

with open(inputFile) as f:
    line = f.readline()

    while(line):
        lastFewLines = lastFewLines[1 if len(lastFewLines) > 3 else 0:] + [line]
        for p in pagesToFind:
            if re.search(f'^    <id>{p}</id>', line):
                pagesToFind.remove(p)
                # write the last few lines to get the start <page>
                for l in lastFewLines:
                    sys.stdout.write(l)
                lastFewLines = []

                # write the rest of this page
                rest = f.readline()
                while(rest):
                    sys.stdout.write(rest)
                    if re.search('</page>', rest):
                        break
                    rest = f.readline()
        if not len(pagesToFind):
            break
        line = f.readline()

print('************************************************')
print('Make sure to check the lines around the <page> elements and insert header and footer')
