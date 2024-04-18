import matplotlib.pyplot as plt

s_values = [0, 0.5, 1, 1.5]
cache_miss_rates = [51.436, 45.104, 42.935, 11.818]
time_usage = [57.170, 55.807, 39.904, 12.833]

fig, ax1 = plt.subplots()

color = 'tab:red'
ax1.set_xlabel('s Values')
ax1.set_ylabel('Cache Miss Rate (%)', color=color)
ax1.plot(s_values, cache_miss_rates, 'o-', color=color)
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()
color = 'tab:blue'
ax2.set_ylabel('Time Usage (ms)', color=color)
ax2.plot(s_values, time_usage, 's--', color=color)
ax2.tick_params(axis='y', labelcolor=color)

plt.grid()

plt.title('Cache Miss Rates and Time Usage by S Values')
fig.tight_layout()
plt.show()
