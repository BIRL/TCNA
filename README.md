# TCNA - The Cancer Noise Atlas
TCNA is a multi-omics noise analysis tool for exploring genomic variability across multiple spatio-temporal scales. It supports noise analysis at genic, pathway, and tumor level across 26 cancer sites using GDC datasets or user uploaded data.
The frontend is built with Vite, TypeScript, React, shadcn-ui, Tailwind CSS and the backend is built with Python FastAPI and RESTful API.

# Set up
To run this on your local system, ensure the following is downloaded on your system:  

- Node.js and npm (if not use [install with nvm](https://github.com/nvm-sh/nvm#installing-and-updating))
- Python 3.11+
- MariaDB 12.0.2


Once the above requirements are fulfilled, you can run the website by following these steps:

### Backend Setup:
```sh
# Step 1: Clone the repository using the project's Git URL.
git clone <GIT_URL>

# Step 2: Navigate to the project directory.
cd <TCNA/Deployed_Server/backend/scripts>

# Step 3: Set up a Python virtual environment
python -m venv env

# Step 4: Activate it:
source env/bin/activate # on Linux-based systems
env\Scripts\activate # on Windows

# Step 5: Install the libraries required:
pip install -r requirements.txt

# Step 6: Launch the server by running the command below:
python app.py
```

### Frontend Setup:
Now in a separate terminal, do the following:
```sh
# Step 1: Navigate to the project directory.
cd <TCNA/Deployed_Server>

# Step 2: Install the necessary dependencies.
npm i

# Step 3: Start the development server with auto-reloading and an instant preview.
npm run dev
```

The website will now be available at [http://localhost:8081](http://localhost:8081).
