import { useEffect, useState } from "react"
import { SectionEnum } from "@components/specialties/enums/specialities.enum"
import "@components/specialties/styles/specialties.css"
import {type IOption, type IQuestions } from "@components/specialties/interfaces/specialties.interface"
import FAQ from "@components/specialties/utils/specialties.service";


export default function Options(){
    
    const [section, setSection] = useState<string>(SectionEnum.Laboral)
    const [options, setOptions] = useState<IOption[]>(FAQ.list_options(SectionEnum.Laboral));
    const [questions, setQuestions] = useState<IQuestions | null>(null)
    const [selectedQuestion, setSelectedQuestion] = useState("");


    useEffect(()=>{
        console.log(questions)
    },[questions])


    
    const handleSection = (section: string) => {
        let options: IOption[] = FAQ.list_options(section);
        setSection(section);
        setOptions(options);
        setQuestions(null);
    }   

    const handleSelection = (alias: string) => {
        setQuestions(FAQ.list_faq(section, alias));
        console.log(questions)
    }

    const handleQuestion = (question: string) => {

        if(question == selectedQuestion){
            setSelectedQuestion("");
        }

        setSelectedQuestion(question);
    }

    return(

        <section className="full center-col-nomid gap-20">

            <div>

                <button className=" text-white h-full border-zinc-600 grow border-r-2 border-l-2 rounded-l-lg hover:bg-blue-600 focus:bg-blue-600 p-3" onClick={()=>{handleSection(SectionEnum.Laboral)}}>LABORAL</button>
                <button className=" text-white h-full border-zinc-600 grow border-r-2 hover:bg-blue-600 focus:bg-blue-600 p-3" onClick={()=>{handleSection(SectionEnum.Civil)}}>CIVIL</button>
                <button className=" text-white h-full border-zinc-600 grow border-r-2 rounded-r-lg hover:bg-blue-600 focus:bg-blue-600 p-3" onClick={()=>{handleSection(SectionEnum.Familiar)}}>FAMILIAR</button>

            </div>
            

            {questions == null ?
            <section className="full center flex-wrap gap-5 laptop:w-[65rem] laptop:h-[45rem] ">

                {
                    options.map((option, index)=>(
                        <div key={index} className="center w-80 h-80 rounded-lg bg-black relative cursor-pointer" onClick={()=>{handleSelection(option.alias)}}>
                            <h2 className="uppercase font-bold text-3xl text-center z-10">{option.title}</h2>
                            <img src={`/static/${option.imageName}`} className="w-full h-full absolute opacity-65 rounded-lg"></img>
                            
                        </div>
                    ))
                }
                
            </section>

            :
            
            <section className="center-col-nomid overflow-scroll gap-10 w-[65rem] h-[45rem] ">

                {
                    (questions.options).map((question, index)=>(
                        // <div key={index} className="w-full">
                        //     <button className="w-full h-32 bg-slate-700" onClick={()=>{setSelectedQuestion(Object.keys(question)[0])}}>{Object.keys(question)[0]}</button>
                        //     {selectedQuestion ==  Object.keys(question)[0] ? <p className="appear bg-red-700 w-full text-center">{Object.values(question)}</p> : null}
                        // </div>

                        <div className="bg-muted rounded-lg border border-muted-foreground p-6 laptop:w-[40rem]">
                            <div className="space-y-4">
                                <h2 className="text-2xl font-bold text-primary-foreground text-center" onClick={()=>{setSelectedQuestion(Object.keys(question)[0])}}>{Object.keys(question)[0]}</h2>
                                {selectedQuestion ==  Object.keys(question)[0] ?
                                <p className="appear center text-muted-foreground ">
                                    {Object.values(question)}
                                </p>
                                :null}
                            </div>
                        </div>
                    ))
                }
                
            </section>
            
            }


        </section>

                    

    )
}