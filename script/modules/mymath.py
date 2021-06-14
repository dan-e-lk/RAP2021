import numpy as np
import scipy.stats


def mean_std_ci(data, confidence=0.95):
    """data must be in a dictionary form (but only the values will be evaluated)
    """
    data = list(data.values())
    a = 1.0 * np.array(data)
    n = len(a)
    if n < 2:
        return_value = {} # used to be 'no data'
    else:
        m, se = np.mean(a), scipy.stats.sem(a)
        ci = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
        ci = round(ci,4)
        std = round(scipy.stats.tstd(a),4)

        return_value = {'mean':round(m,4), 'stdv':std, 'ci':ci, 'upper_ci':round(m+ci,4), 'lower_ci':round(m-ci,4), 'n':n, 'confidence':confidence}

    return return_value


def check_duplicates(lst):
    """ input: any list
        output: None if all values in the list are unique
            a list of duplicate values if the list contains duplicate values
    """
    if len(lst) == len(set(lst)):
        return None
    else:
        unique_vals = list(set(lst))
        unique_vals_dict = {val:0 for val in unique_vals}
        for i in lst:
            unique_vals_dict[i] += 1
        dupl_lst = [val for val, count in unique_vals_dict.items() if count > 1]
        dupl_str = ''
        for i in dupl_lst:
            dupl_str += str(i) + ', '
        dupl_str = dupl_str[:-2]

        return [dupl_lst, dupl_str]


if __name__ == '__main__':
    data1 = {'1':3,'2':3,'3':3,'4':4,'5':4,'6':4,'7':5,'8':5,'9':5,'10':6}
    data2 = {'1':0.5,'2':0.5,'3':0.5,'4':0.6,'5':0.6,'6':0.6,'7':0.8,'8':0.8,'9':0.8,'10':1}
    data3 = {'1':0.5}
    print(mean_std_ci(data1))
    print(mean_std_ci(data2))
    print(mean_std_ci(data3))


    # data3 = [1,2,3,4,5,6,7,8]
    # data4 = [1,1,2,2,3,4,5,6,7]

    # print(check_duplicates(data3))
    # print(check_duplicates(data4))    
