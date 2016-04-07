#!/usr/bin/env python2.7

"""
Read the file agents.raw and output agents.txt, a readable file
"""

if __name__=="__main__":
    with open("agents.raw","r") as f:
        with open("agents.txt","w") as out:
            for line in f:
                if(line[0] in '+|'):
                    out.write(line)

            
