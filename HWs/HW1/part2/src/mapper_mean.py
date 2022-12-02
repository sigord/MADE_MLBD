#!/usr/bin/env python3
import sys
import csv

def main():
    length = 0
    value = 0.0
    
    data = csv.reader(sys.stdin, delimiter=',')
    for line in data:
        price = line[9]
        if price.isdigit():
            length += 1
            value += float(price)
    
    print(length, value/length, sep='\t')
    
if __name__ == "__main__":
    main()