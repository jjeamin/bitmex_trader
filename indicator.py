import numpy as np
import talib


# Ehlers Stochastic CG Oscillator [LazyBear]
# Implement trading view indicator

def ESCGO(df, p_len=8):
    d_len = len(df)

    hl2 = np.array((df.high + df.low) / 2)

    nm = [0] * d_len
    dm = [0] * d_len
    cg = [0] * d_len
    v1 = [0] * d_len
    v2 = [0] * d_len
    v3 = [0] * d_len
    t = [0] * d_len

    for i in range(p_len-1, d_len):
        for j in range(0, p_len):
            nm[i] += (j + 1) * hl2[i - j]
            dm[i] += hl2[i - j]

        cg[i] = -nm[i] / dm[i] + (p_len + 1) / 2.0 if dm[i] != 0 else 0

    cg = np.array(cg)
    min_value, max_value = talib.MINMAX(cg, timeperiod=p_len)

    for i in range(p_len-1, d_len):
        v1[i] = (cg[i] - min_value[i]) / (max_value[i] - min_value[i]) if max_value[i] is not min_value[i] else 0
        v2[i] = (4 * v1[i] + 3 * v1[i - 1] + 2 * v1[i - 2] + v1[i - 3]) / 10.0
        v3[i] = 2 * (v2[i] - 0.5)
        t[i] = (0.96 * ((v3[i - 1]) + 0.02))

    return v3, t
