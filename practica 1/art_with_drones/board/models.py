from django.db import models

# Create your models here.
# board/models.py

class DronePosition(models.Model):
    x = models.IntegerField()
    y = models.IntegerField()
    order = models.IntegerField()
