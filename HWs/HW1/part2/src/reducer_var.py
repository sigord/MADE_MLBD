#!/usr/bin/env python3
import sys

def main():
    total_length = 0
    total_mean = 0.0
    total_var = 0.0
    
    for line in sys.stdin:
        length, mean, var = line.split('\t')
        length, mean, var = int(length), float(mean), float(var)
        left_summ = (total_var * total_length + var * length)/(total_length + length)
        right_summ = total_length * length * ((total_mean - mean)/(total_length + length))**2
        total_var = left_summ + right_summ
        total_mean = (total_mean * total_length + mean * length) / (total_length + length)
        total_length += length
        
    
    print(total_length, total_mean, total_var, sep='\t')
    

if __name__ == "__main__":
    main()