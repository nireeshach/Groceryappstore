# Generated by Django 2.0.5 on 2018-05-24 10:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('masterdata', '0007_ingredient_isfinalspec'),
    ]

    operations = [
        migrations.AlterField(
            model_name='ingredient',
            name='notes',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
    ]
