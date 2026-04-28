import kagglehub

# Download latest version
path = kagglehub.dataset_download("tiagoadrianunes/imdb-top-5000-movies")

print("Path to dataset files:", path)