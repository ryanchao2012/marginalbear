import fileinput
# parallel -j3 --pipe --round-robin --block 10K --line-buffer

'''
| pos   | count   |
|-------+---------|
| n     | 231297  |
| url   | 130987  |
| nr    | 125550  |
| v     | 125366  |
| eng   | 80033   |
| l     | 25745   |
| ns    | 24247   |
| m     | 24097   |
| i     | 22565   |
| nz    | 18032   |
| a     | 17048   |
| d     | 10340   |
| nrt   | 7490    |
| vn    | 6327    |
| z     | 5079    |
| t     | 4227    |
| nt    | 4162    |
| b     | 3588    |
| j     | 3276    |
| zg    | 2667    |
| r     | 2145    |
| x     | 1841    |
| c     | 1642    |
| yg    | 1550    |
| s     | 1324    |
| nrfg  | 1097    |
| y     | 994     |
| g     | 859     |
| f     | 780     |
| ng    | 732     |
| o     | 517     |
| vg    | 490     |
| q     | 486     |
| ad    | 416     |
| p     | 296     |
| ag    | 129     |
| mq    | 119     |
| an    | 99      |
| u     | 81      |
| e     | 68      |
| tg    | 21      |
| dg    | 8       |
| mg    | 7       |
| vd    | 5       |
| k     | 5       |
| vi    | 3       |
| rg    | 3       |
| h     | 2       |
| uz    | 2       |
| ug    | 2       |
| bg    | 2       |
| rr    | 2       |
| vq    | 2       |
| rz    | 2       |
| ul    | 2       |
| uj    | 1       |
| df    | 1       |
| ud    | 1       |
| uv    | 1       |
+-------+---------+
'''

available_pos = ['n', 'nr', 'v', 'eng', 'l', 'ns', 'm', 'i', 'nz', 'a', 'd', 'nrt', 'vn', 't', 'nt', 'r', 'c', 'nrfg']


for line in fileinput.input():
    try:
        fields = line.split('\t')
        tag = fields[0]
        tokenized = fields[1].split()
        grammar = fields[2].split()
        filtered = ' '.join([tokenized[i] for i, pos in enumerate(grammar) if pos in available_pos])
        if bool(filtered.strip()):
            print('{}\t{}'.format(tag, filtered))

    except:
        pass
    
