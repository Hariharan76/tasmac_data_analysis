from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import matplotlib.pyplot as plt
from io import BytesIO
import base64

app = FastAPI()

# Example function to generate and save a matplotlib plot
def generate_plot():
    x = [1, 2, 3, 4, 5]
    y = [10, 20, 15, 25, 30]
    
    plt.figure(figsize=(8, 6))
    plt.plot(x, y, marker='o')
    plt.title('Example Plot')
    plt.xlabel('X-axis')
    plt.ylabel('Y-axis')
    
    # Save plot to a bytes buffer
    buffer = BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    
    # Encode plot image to base64
    plot_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    buffer.close()
    
    return plot_base64

# Endpoint to render the plot image
@app.get("/plot", response_class=HTMLResponse)
async def get_plot():
    plot_base64 = generate_plot()
    
    # HTML content to display the image in the browser
    html_content = """
    <html>
    <head><title>Matplotlib Plot</title></head>
    <body>
    <h2>Matplotlib Plot</h2>
    <img src="data:image/png;base64,{}" />
    </body>
    </html>
    """.format(plot_base64)
    
    return html_content
