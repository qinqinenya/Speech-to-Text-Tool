import io, os, sys, time, shutil, argparse

"""Imports the Google Cloud client library"""
from google.cloud import speech
from google.cloud.speech import enums
from google.cloud.speech import types
from google.cloud import storage

video_directory = "./video"
audio_directory = "./audio"
text_directory = "./text"
bucket_name = "for-anny-only"

debug_progress = True

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-l", "--language", type=str,
                    choices=["yue", "zh", "hk", "tw", "en"], default="yue",
                    help="indicate the language in the video.\n"
                    "* (default) \"yue\" refers to Chinese, Cantonese (Traditional, Hong Kong)\n"
                    "* \"zh\" refers to Chinese, Mandarin (Simplified, China)\n"
                    "* \"hk\" refers to Chinese, Mandarin (Simplified, Hong Kong)\n"
                    "* \"tw\" refers to Chinese, Mandarin (Traditional, Taiwan)\n"
                    "* \"en\" refers to English (United States)")
parser.add_argument("-c", "--clean", type=bool, const=True, default=False, nargs="?",
                    help="clean up all the contents in the video directory and \n"
                    "in the cloud bucket, as well as delete the audio and text directory.")
parser.add_argument("-r", "--recognize", type=bool, const=True, default=False, nargs="?",
                    help="only conduct speech recognition to audio files \n"
                    "located in cloud bucket.")

def debug_info(*arg):
    if debug_progress:
        print(">>>", *arg)

def debug(f):
    def inner(*args, **kargs):
        debug_info("start " + f.__name__)
        start_time = time.time()
        ret = f(*args, **kargs)
        elapsed = time.time() - start_time
        debug_info("end " + f.__name__ +
                   " \t==> (%.2f seconds spent)" % elapsed)
        return ret
    return inner

def print_message(message):
    print("==================================================")
    print(message)
    print("==================================================")

def print_notification(message):
    print_message(message)
    print("\n\n");

def get_file_extension(file_name):
    return os.path.splitext(file_name)[1].lower()[1:]

def set_speech_recognition_config(language_code):
    return types.RecognitionConfig(
           encoding=enums.RecognitionConfig.AudioEncoding.FLAC,
           language_code=language_code)

def load_audio_into_memory(gcs_uri):
        return types.RecognitionAudio(uri=gcs_uri)

def get_language_code(language):
    if (language == "zh"):
        return "zh"
    if (language == "hk"):
        return "zh-HK"
    if (language == "tw"):
        return "zh-TW"
    if (language == "en"):
        return "en-US"
    return "yue-Hant-HK"

@debug
def check_directory():
    """Check if video directory exists."""
    if not os.path.exists(video_directory):
        message = "Cannot find the video directory.\n"\
            "Please create a directory named 'video',\n"\
            "and put your mp4 videos inside it."
        print_message(message)
        sys.exit(0)
        
    """Check if video directory has at least one valid mp4 video."""
    has_at_least_one_valid_video = False
    for file_name in os.listdir(video_directory):
        if (get_file_extension(file_name) == "mp4"):
            has_at_least_one_valid_video = True

    if not has_at_least_one_valid_video:
        message = "Please put mp4 videos in this directory."
        print_message(message)
        sys.exit(0)

    """Create audio directory if not exists."""
    if not os.path.exists(audio_directory):
        os.mkdir(audio_directory)

    """Create text directory if not exists."""
    if not os.path.exists(text_directory):
        os.mkdir(text_directory)

@debug
def extract_audio_from_video():
    extracted_file_names = []
    for file_name in os.listdir(video_directory):
        if (get_file_extension(file_name) == "mp4"):
            video_path = os.path.join(os.path.abspath(video_directory),
                file_name)
            audio_path = os.path.join(os.path.abspath(audio_directory), "{}.flac".format(file_name[:-4]))
            
            """If the audio already exists, skip it."""
            if not os.path.exists(audio_path):
                """Convert mp4 to flac.
                   * “-i” refers to input.
                   * “-ac” sets the number of audio channels.
                   * “-sample_fmt” sets the audio sample format."""
                os.system("ffmpeg -i {0} -ac 1 -sample_fmt s16 -filter_threads 4 {1}"\
                          .format(video_path, audio_path))
                extracted_file_names.append(file_name)
    return extracted_file_names

@debug
def upload_audio_to_cloud_storage():
    """Lists all the audio blobs in the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    blob_name_set = set()
    for blob in blobs:
        blob_name_set.add(blob.name)

    uploaded_file_names = []
    for file_name in os.listdir(audio_directory):
        if (get_file_extension(file_name) == "flac"):
            audio_path = os.path.join(os.path.abspath(audio_directory),
                file_name)
                
            """If the audio already exists, skip it."""
            if file_name not in blob_name_set:
                print("Uploading {}...".format(file_name))
                blob = bucket.blob(file_name)
                blob.upload_from_filename(audio_path)
                uploaded_file_names.append(file_name)
    return uploaded_file_names

@debug
def recognize_speech_from_audio(language):
    recognized_file_names = []
    
    speech_client = speech.SpeechClient()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        if (get_file_extension(blob.name) == "flac"):
            audio_path = os.path.join(*["gs://", bucket_name, blob.name])
            if not os.path.exists(text_directory):
                os.mkdir(text_directory)
            text_path = os.path.join(os.path.abspath(text_directory),
                                     "{}.txt".format(blob.name[:-5]))

            config = set_speech_recognition_config(get_language_code(language))
            audio = load_audio_into_memory(audio_path)
            # Detects speech in the audio file
            operation = speech_client.long_running_recognize(config, audio)

            print("Processing {} for speech recognition...".format(blob.name))
            response = operation.result()
            
            with open(text_path, "w+") as text_file:
                # Each result is for a consecutive portion of the audio. Iterate through
                # them to get the transcripts for the entire audio file.
                for result in response.results:
                    # The first alternative is the most likely one for this portion.
                    text_file.write(u'{}\n'.format(result.alternatives[0].transcript))

            recognized_file_names.append(blob.name)
    return recognized_file_names

def clean():
    if os.path.exists(video_directory):
        for file_name in os.listdir(video_directory):
            video_path = os.path.join(os.path.abspath(video_directory),
                                      file_name)
            os.remove(video_path)
    
    if os.path.exists(audio_directory):
        shutil.rmtree(audio_directory)

    if os.path.exists(text_directory):
        shutil.rmtree(text_directory)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    for blob in blobs:
        blob.delete()

def main():
    args = parser.parse_args()
    if args.clean:
        clean()
        message = "Clean up work completed."
        print_notification(message)
        sys.exit(0)
    
    if args.recognize:
        recognized_file_names = recognize_speech_from_audio(args.language)
        if not recognized_file_names:
            message = "No audio needs speech recognition."
        else:
            message = "Speech recognition completed (flac to txt).\n{}"\
                .format("\n".join(recognized_file_names))
        print_notification(message)
        sys.exit(0)

    start_time = time.time()

    check_directory()
    message = "Initial directory check completed."
    print_notification(message)

    extracted_file_names = extract_audio_from_video()
    if not extracted_file_names:
        message = "No video needs audio extraction."
    else:
        message = "Audio extraction completed (mp4 to flac).\n{}"\
            .format("\n".join(extracted_file_names))
    print_notification(message)

    uploaded_file_names = upload_audio_to_cloud_storage()
    if not uploaded_file_names:
        message = "No audio needs upload."
    else:
        message = "Audio upload completed.\n{}"\
            .format("\n".join(uploaded_file_names))
    print_notification(message)

    recognized_file_names = recognize_speech_from_audio(args.language)
    if not recognized_file_names:
        message = "No audio needs speech recognition."
    else:
        message = "Speech recognition completed (flac to txt).\n{}"\
            .format("\n".join(recognized_file_names))
    print_notification(message)

    elapsed = time.time() - start_time
    debug_info("In total, %.2f seconds spent." % elapsed)

if __name__ == '__main__':
    main()
