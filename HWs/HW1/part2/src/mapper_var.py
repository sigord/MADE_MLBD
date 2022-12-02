#!/usr/bin/env python3
import sys
import csv

def main():
    length = 0
    mean = 0.0
    var = 0.0
    
    data = csv.reader(sys.stdin, delimiter=',')
    for line in data:
        price = line[9]
        if price.isdigit():
            price = float(price)
            
            var = length * (var + ((mean - price)**2)/(length + 1))/(length + 1)
            mean = (mean * length + price) / (length + 1)
            length += 1
    
    print(length, mean, var,  sep='\t')
    
if __name__ == "__main__":
    main()