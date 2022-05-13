#!/bin/bash


# inteiro que identifica a rodada de dados a ser processados
# exemplo: 0 -> processa dados de 0 a 1629.
#          1 -> processa dasdos de 1630 até 1630*2 -1
let batch=0


# Enquanto não encontrar um arquivo chamado StopSign, execute o script main.
while [ ! -f "StopSign" ];
do
    setsid nohup python main.py $batch > main.log 2>&1 < main.log &
    ((++batch))
    sleep 10
done