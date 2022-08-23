import argparse
def getArgs():
    parser = argparse.ArgumentParser(description='Validation Framework')
    parser.add_argument('-s','--sourceURL',help='Contains Source URL')
    parser.add_argument('-t','--targetURL',help='Contains Target URL')
    args = parser.parse_args()
    return args


def main(args):
    s = 'Hi {sourceURL}, {targetURL}'.format(**vars(args))
    print(vars(args))
    #s='a'
    print(s)
    a = 'a,b, c , d ,e'
    for i in map(strip(), a.split(',')):
        print(i)

if __name__ == '__main__':
    args = getArgs()
    main(args)
