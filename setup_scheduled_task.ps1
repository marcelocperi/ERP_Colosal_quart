# Definir rutas
$ProjectDir = "C:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
$VbsPath = "$ProjectDir\launch_colosal.vbs"

# Nombre de la tarea
$TaskName = "Colosal_ERP_Server"

# Configurar la acción (ejecutar el script invisible)
$Action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"$VbsPath`"" -WorkingDirectory $ProjectDir

# Configurar el trigger (al iniciar el sistema)
$Trigger = New-ScheduledTaskTrigger -AtStartup

# Configurar ajustes (reiniciar si falla, permitir ejecución sin AC)
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

# Registrar la tarea como SYSTEM para que corra siempre en segundo plano
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings -User "SYSTEM" -Force

Write-Host "======================================================"
Write-Host "  TAREA PROGRAMADA REGISTRADA EXITOSAMENTE"
Write-Host "======================================================"
Write-Host "La tarea 'Colosal_ERP_Server' iniciará con el sistema."
Write-Host "Para iniciarla ahora mismo manualmente:"
Write-Host "Start-ScheduledTask -TaskName '$TaskName'"
