from core.utils import (
    rm_repeat,
    to_halfwidth
)


def test_rm_repeat():
    samples = [
        ('aaaaaaaaaaaaaaaaaaaaa', 'aa'),
        ('QQQQQQQ', 'QQ'),
        ('XDDDDD', 'XDD'),
        ('XXXDDDDD', 'XXDD'),
        ('。。。。。？？？', '。。？？'),
        ('滾滾滾滾', '滾滾'),
        ('姆咪姆咪姆咪', '姆咪'),
        ('............????????', '..??'),
        ('一例一休一例一休一例一休一例一休一例一休', '一例一休'),
        ('印和癲印和癲印和癲印和癲印和癲', '印和癲')
    ]

    for s in samples:
        assert rm_repeat(s[0]) == s[1]


def test_to_halfwidth():
    samples = [
        ('？！，', '?!,'),
        ('。', '。'),
        ('＠＃＄％＾＆＊（）＿＋', '@#$%^&*()_+'),
        ('ＡＢＣＤ', 'ABCD'),
        ('、', '、'),
        ('［］', '[]')
    ]

    for s in samples:
        assert to_halfwidth(s[0]) == s[1]
