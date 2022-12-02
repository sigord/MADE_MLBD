#!/usr/bin/env python3
import sys

def main():
    total_length = 0
    total_mean = 0.0
    
    for line in sys.stdin:
        length, mean = line.split('\t')
        length, mean = int(length), float(mean)
        total_mean = (total_mean * total_length + mean * length) / (total_length + length)
        total_length += length
        
    
    print(total_length, total_mean, sep='\t')
    

if __name__ == "__main__":
    main()