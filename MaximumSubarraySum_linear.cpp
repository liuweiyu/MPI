//
//  main.cpp
//  max_subarr_sum_Linear
//
//  Created by hang shao on 9/27/14.
//  Copyright (c) 2014 hang shao. All rights reserved.
//
#include <stdio.h>
#include <algorithm>
#include <stdlib.h>
#include<iostream>
int Max_Subarray_Linear(int *arr,int n)
{
    int sum = 0;
    int current_max = 0;
    for (int k = 1; k <= n ; k++)
    {
        if (sum + arr[k] > 0)
        {
            sum = sum + arr[k];
            if (sum > current_max)
            {
                current_max = sum;
            }
        }
        else
            sum = 0;
    }
    return current_max;
}

int main()
{
    int n;
    n = 100;
    int *arr = new int [n];
    for (int i = 0; i < n; i++)
    {
        arr[i] = rand() % 200 + -100;
    }
    int max_sum = Max_Subarray_Linear(arr,n);
    printf("Maximum subarray sum is %d\n", max_sum);
    getchar();
    delete arr;
    return 0;
}
