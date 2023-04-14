import subprocess
# Bringing this elsewhere to run
# patients_w_waveform = [41031, 42995, 46651, 57321, 58505, 66093, 68808, 74711, 79096,  81694, 93360, 94255]

patients_w_waveform = [57321,  93360, 58505, 74711]
# Download the waveform file of all patients in matched dataset
cmd = "ls "
idx = 0

# Download all the patient records in parallel
while idx <= len(patients_w_waveform) -1:
    patient = f'p{patients_w_waveform[idx]:06d}'
    cmd += f"&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/{patient[0:3]}/{patient}/ -P data/waveform"
    if True: # idx%5==0:
        print(f'Done with {idx}!')
        print(cmd)
        process = subprocess.Popen(cmd, shell=True)
        cmd = "ls "
    idx+=1  

    # pad a string with 0


process = subprocess.Popen(cmd, shell=True)
print(cmd)

# cd project
# ls && /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p04/p044298/ -P data/waveform
# Done with 5!
# ls && /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p06/p063762/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p04/p043926/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p08/p086678/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p08/p083873/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p05/p054183/ -P data/waveform
# Done with 10!
# ls && /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p09/p099756/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p05/p052529/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p09/p096305/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p08/p082104/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p09/p096060/ -P data/waveform
# Done with 15!
# ls && /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p07/p070723/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p04/p046154/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p04/p041035/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p04/p046927/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p08/p082512/ -P data/waveform
# Done with 20!
# ls && /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p05/p050015/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p06/p060659/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p06/p062323/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p05/p056697/ -P data/waveform&& /Users/michaelscott/opt/anaconda3/bin/wget -r -N -c -np https://physionet.org/files/mimic3wdb-matched/1.0/p08/p087675/ -P data/waveform