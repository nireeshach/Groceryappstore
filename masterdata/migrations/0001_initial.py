# Generated by Django 2.0.5 on 2018-05-21 11:23

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Category',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'category',
            },
        ),
        migrations.CreateModel(
            name='Characteristic',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('alternates', models.TextField(blank=True, null=True)),
                ('additional_info', models.TextField(blank=True, null=True)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'characteristic',
            },
        ),
        migrations.CreateModel(
            name='CharacteristicType',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'characteristic_type',
            },
        ),
        migrations.CreateModel(
            name='Group',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'group',
            },
        ),
        migrations.CreateModel(
            name='Ingredient',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('alternates', models.TextField(blank=True, null=True)),
                ('additional_info', models.TextField(blank=True, null=True)),
                ('validated', models.BooleanField(choices=[(True, 'Yes'), (False, 'No')], default=False)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
                ('category', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='masterdata.Category')),
                ('group', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='masterdata.Group')),
            ],
            options={
                'db_table': 'ingredient',
            },
        ),
        migrations.CreateModel(
            name='State',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('createdate', models.DateTimeField(auto_now_add=True)),
                ('lastmodified', models.DateTimeField(auto_now=True)),
            ],
            options={
                'db_table': 'state',
            },
        ),
        migrations.AddField(
            model_name='ingredient',
            name='state',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='masterdata.State'),
        ),
        migrations.AddField(
            model_name='characteristic',
            name='type',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='masterdata.CharacteristicType'),
        ),
    ]
