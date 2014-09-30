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
int max_thr(int a, int b, int c) { return std::max(std::max(a, b), c); }
int maxCrossingSum(int *&arr, int l, int m, int h)
{

    int sum = 0;
    int left_sum = INT_MIN;
    for (int i = m; i >= l; i--)
    {
        sum = sum + arr[i];
        if (sum > left_sum)
            left_sum = sum;
    }
    sum = 0;
    int right_sum = INT_MIN;
    for (int i = m+1; i <= h; i++)
    {
        sum = sum + arr[i];
        if (sum > right_sum)
            right_sum = sum;
    }
    return left_sum + right_sum;
}
int maxSubArraySum(int *&arr, int l, int h)
{
    if (l == h)
        return arr[l];
    int m = (l + h)/2;
    return max_thr(maxSubArraySum(arr, l, m),
               maxSubArraySum(arr, m+1, h),
               maxCrossingSum(arr, l, m, h));
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
    int max_sum = maxSubArraySum(arr, 0, n-1);
    printf("Maximum contiguous sum is %d\n", max_sum);
    getchar();
    delete arr;
    return 0;
}
