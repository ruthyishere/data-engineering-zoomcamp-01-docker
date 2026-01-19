## This creates a Docker image, using Python image as a base, and installs pandas and pyarrow libraries.
## A working directory /code is created -- this is where the code will live inside any container created from this image.
## The pipeline.py file is copied from the host machine into the /code directory in the container.


FROM python:3.9-slim 

## Copy the uv binary from the uv image to enable running uv commands in this container
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/ 

WORKDIR /code 
## Create a virtual environment in the /code/.venv directory
ENV PATH="/code/.venv/bin:$PATH"

## Copy the dependency files to the working directory
COPY pyproject.toml .python-version uv.lock ./

## Install the dependencies specified in the uv.lock file
RUN uv sync --locked 

# RUN pip install pandas pyarrow 


## Copy the pipeline.py file into the working directory in the container
COPY ingest_data.py . 

ENTRYPOINT ["python", "ingest_data.py"]

# ## Set the entrypoint to run the pipeline.py script using uv
# ENTRYPOINT ["uv", "run", "python", "pipeline.py"]

## docker build -t test:pandas . --> builds the image with the name test and tag pandas
## docker run --rm -it test:pandas --> runs the container interactively and removes it after closing container