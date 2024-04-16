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
    'uq': [1.654, 56.340],
    'uhd': [0.828, 10.355],
    'uhs': [0.828, 10.299],
    'lq': [1.644, 32.956],
    'lhd': [0.825, 8.418],
    'lhs': [0.845, 8.955],
    'hq': [1.643, 15.238],
    'hhd': [0.838, 18.036],
    'hhs': [0.837, 18.192]
}

plot_execution_modes(example_data)
