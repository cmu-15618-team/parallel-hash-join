import matplotlib.pyplot as plt

def plot_execution_modes(data, title):
    modes = list(data.keys())
    build_times = [data[mode][0] for mode in modes]
    probe_times = [data[mode][1] for mode in modes]

    fig, ax = plt.subplots()
    ax.bar(modes, build_times, label='Build', color='black')
    ax.bar(modes, probe_times, bottom=build_times, label='Probe', color='grey')

    ax.set_xlabel('Execution Modes')
    ax.set_ylabel('Time Cost (s)')
    ax.set_title(f'Time Cost by Execution Mode and Stage ({title})')
    ax.grid(True)
    plt.legend()

    plt.savefig(f'../img/{title.replace(" ", "_")}.png')

example_data_u = {
    'Sequential': [1.641, 55.883],
    'Shared Dynamic': [0.869, 7.468],
    'Shared Static': [0.826, 7.606]
}

example_data_l = {
    'Sequential': [1.639, 32.105],
    'Shared Dynamic': [0.832, 4.415],
    'Shared Static': [0.821, 4.378]
}

example_data_h = {
    'Sequential': [1.655, 13.808],
    'Shared Dynamic': [0.814, 1.951],
    'Shared Static': [0.859, 1.873]
}

plot_execution_modes(example_data_u, "Uniform Distribution")
plot_execution_modes(example_data_l, "Low Skew Distribution")
plot_execution_modes(example_data_h, "High Skew Distribution")
