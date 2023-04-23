from bokeh.client import push_session
from bokeh.models import ColumnDataSource
import random

# Define the URL of the Bokeh server application
url = 'http://localhost:5006/plot'

# Define the name of the plot to update
plot_name = 'my_plot'

# Define the update interval in milliseconds
update_interval = 1000

# Connect to the Bokeh server application
session = push_session(document=None, session_id=None, url=url)

# Get the plot data source
source = session.document.select(plot_name)

source = list(source)

for i in source:
    print(i)

print(source)
# Define the update function
def update():
    # Generate some random data
    x = [random.randint(0, 100) for i in range(10)]
    y = [random.randint(0, 100) for i in range(10)]

    # Update the plot data source with new data
    new_data = {'x': x, 'y': y}
    source.stream(new_data)

# Add a periodic callback to update the plot every second
session.document.add_periodic_callback(update, update_interval)

# Start the session
session.show()

# Run forever
session.loop_until_closed()