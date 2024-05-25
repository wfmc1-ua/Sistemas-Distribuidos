# board/views.py

from django.shortcuts import render
from django.http import JsonResponse
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['SD']
collection = db['Tablero']
def board_view(request):
    positions = [

    ]
    rows = range(1, 21)
    cols = range(1, 21)
    return render(request, 'board/board.html', {'positions': positions, 'rows': rows, 'cols': cols})
def update_positions(request):
    if request.method == 'POST':
        # Asume que los datos vienen en formato JSON
        new_positions = request.POST.getlist('positions[]')
        new_positions = [tuple(map(int, pos.split(','))) for pos in new_positions]

        # Aquí podrías guardar las nuevas posiciones en la base de datos MongoDB si es necesario
        for pos in new_positions:
            collection.insert_one({'row': pos[0], 'col': pos[1], 'value': pos[2]})

        return JsonResponse({'status': 'success'})
    return JsonResponse({'status': 'failed'})