import matplotlib.pyplot as plt

def plot_execution_modes(data):
    modes = list(data.keys())
    build_times = [data[mode][0] for mode in modes]
    probe_times = [data[mode][1] for mode in modes]

    fig, ax = plt.subplots()
    ax.bar(modes, build_times, label='Build', color='black')
    ax.bar(modes, probe_times, bottom=build_times, label='Probe', color='grey')

    ax.set_xlabel('Execution Modes')
    ax.set_ylabel('Time Cost (s)')
    ax.set_title('Time Cost by Execution Mode and Stage')
    ax.grid(True)
    plt.legend()

    plt.show()

example_data = {
    'uq': [1.641, 55.883],
    'uhd': [0.869, 7.468],
    'uhs': [0.826, 7.606],
    'lq': [1.639, 32.105],
    'lhd': [0.832, 4.415],
    'lhs': [0.821, 4.378],
    'hq': [1.655, 13.808],
    'hhd': [0.814, 1.951],
    'hhs': [0.859, 1.873]
}

plot_execution_modes(example_data)
