

# Environment setup


## 1. Clone the projet
git clone https://github.com/TON-USERNAME/driving-license-analysis.git

## 2. go to project folder
cd driving-license-analysis

## 3. create a .env file based on .env.example
cp .env.example .env

## 4. update .env file with personnal login ID 
 (open .env and replace values + save)

## 5. create and activate the virtual environnement

#### On Windows 

python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

#### On macOS / Linux 
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


## 6. install all dependencies
pip install -r requirements.txt